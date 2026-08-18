/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.materializedtable.workflow;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.workflow.CreatePeriodicRefreshWorkflow;
import org.apache.flink.table.workflow.CreateRefreshWorkflow;
import org.apache.flink.table.workflow.DeleteRefreshWorkflow;
import org.apache.flink.table.workflow.ModifyRefreshWorkflow;
import org.apache.flink.table.workflow.ResumeRefreshWorkflow;
import org.apache.flink.table.workflow.SuspendRefreshWorkflow;
import org.apache.flink.table.workflow.WorkflowException;
import org.apache.flink.table.workflow.WorkflowScheduler;

import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.TriggerBuilder.newTrigger;
import static org.quartz.impl.StdSchedulerFactory.PROP_SCHED_INSTANCE_NAME;
import static org.quartz.impl.StdSchedulerFactory.PROP_SCHED_RMI_EXPORT;
import static org.quartz.impl.StdSchedulerFactory.PROP_SCHED_RMI_PROXY;
import static org.quartz.impl.StdSchedulerFactory.PROP_THREAD_POOL_CLASS;
import static org.quartz.impl.StdSchedulerFactory.PROP_THREAD_POOL_PREFIX;

/**
 * A quartz-backed {@link WorkflowScheduler} that triggers full-mode refreshes in this process.
 *
 * <p>The SQL Gateway's embedded scheduler exists because the gateway is a server: its trigger opens
 * a fresh gateway session over REST and calls the refresh endpoint, so the round trip leaves and
 * re-enters the JVM. Here there is no server to call, so a trigger invokes the materialized table
 * lifecycle directly.
 *
 * <p>Workflows live in memory and last only as long as this scheduler. That makes it suitable for a
 * long-running process — including a Table API program deployed in application mode, where the
 * driver runs inside the JobManager — but a program that exits stops refreshing. In application
 * mode that also requires {@code execution.shutdown-on-application-finish=false}, since a full-mode
 * table has no running job to hold the cluster open between refreshes.
 */
@Internal
public class InProcessWorkflowScheduler implements WorkflowScheduler<InProcessRefreshHandler> {

    private static final Logger LOG = LoggerFactory.getLogger(InProcessWorkflowScheduler.class);

    private static final String WORKFLOW_GROUP = "materialized-table";
    private static final String SCHEDULER_ID = "schedulerId";
    private static final String WORKFLOW_NAME = "workflowName";
    private static final DateTimeFormatter SCHEDULE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Quartz constructs job instances itself, so the trigger cannot be handed a callback directly.
     * Scheduler instances register here under an id carried in the job data map.
     */
    private static final Map<String, InProcessWorkflowScheduler> INSTANCES =
            new ConcurrentHashMap<>();

    private final String schedulerId = UUID.randomUUID().toString();

    /** Refresh actions by workflow name, captured when the workflow is created. */
    private final Map<String, PeriodicRefreshTrigger> triggers = new ConcurrentHashMap<>();

    private Scheduler quartzScheduler;

    /** Invoked when a periodic refresh fires. */
    @FunctionalInterface
    public interface PeriodicRefreshTrigger {
        void refresh(String scheduleTime, Map<String, String> dynamicOptions) throws Exception;
    }

    /**
     * Binds the action to run when {@code workflowName} fires.
     *
     * <p>Separate from {@link #createRefreshWorkflow} because the {@link WorkflowScheduler}
     * contract carries no callback: a scheduler that reaches its engine over the network needs only
     * the workflow description, whereas this one needs the engine itself.
     */
    public void registerTrigger(String workflowName, PeriodicRefreshTrigger trigger) {
        triggers.put(workflowName, trigger);
    }

    @Override
    public void open() throws WorkflowException {
        Properties properties = new Properties();
        properties.setProperty(PROP_SCHED_INSTANCE_NAME, "in-process-mt-scheduler-" + schedulerId);
        properties.setProperty(PROP_SCHED_RMI_EXPORT, "false");
        properties.setProperty(PROP_SCHED_RMI_PROXY, "false");
        properties.setProperty(PROP_THREAD_POOL_CLASS, "org.quartz.simpl.SimpleThreadPool");
        properties.setProperty(PROP_THREAD_POOL_PREFIX + ".threadCount", "1");
        // daemon threads: a TableEnvironment has no close(), so nothing would ever shut this down
        properties.setProperty(PROP_THREAD_POOL_PREFIX + ".makeThreadsDaemons", "true");

        try {
            quartzScheduler = new StdSchedulerFactory(properties).getScheduler();
            quartzScheduler.start();
            INSTANCES.put(schedulerId, this);
            LOG.info("Started in-process materialized table scheduler {}.", schedulerId);
        } catch (org.quartz.SchedulerException e) {
            throw new WorkflowException("Failed to start the in-process refresh scheduler.", e);
        }
    }

    @Override
    public void close() throws WorkflowException {
        INSTANCES.remove(schedulerId);
        triggers.clear();
        if (quartzScheduler == null) {
            return;
        }
        try {
            quartzScheduler.shutdown();
        } catch (org.quartz.SchedulerException e) {
            throw new WorkflowException("Failed to stop the in-process refresh scheduler.", e);
        }
    }

    @Override
    public InProcessRefreshHandlerSerializer getRefreshHandlerSerializer() {
        return InProcessRefreshHandlerSerializer.INSTANCE;
    }

    @Override
    public InProcessRefreshHandler createRefreshWorkflow(
            CreateRefreshWorkflow createRefreshWorkflow) throws WorkflowException {
        if (!(createRefreshWorkflow instanceof CreatePeriodicRefreshWorkflow)) {
            throw new WorkflowException(
                    String.format(
                            "Unsupported create refresh workflow type %s.",
                            createRefreshWorkflow.getClass().getSimpleName()));
        }
        CreatePeriodicRefreshWorkflow periodic =
                (CreatePeriodicRefreshWorkflow) createRefreshWorkflow;
        String workflowName = periodic.getMaterializedTableIdentifier().asSerializableString();

        try {
            scheduleWorkflow(workflowName, periodic.getCronExpression(), Map.of());
        } catch (org.quartz.SchedulerException e) {
            throw new WorkflowException(
                    String.format("Failed to schedule refresh workflow %s.", workflowName), e);
        }
        return new InProcessRefreshHandler(workflowName, WORKFLOW_GROUP);
    }

    @Override
    public void modifyRefreshWorkflow(
            ModifyRefreshWorkflow<InProcessRefreshHandler> modifyRefreshWorkflow)
            throws WorkflowException {
        InProcessRefreshHandler handler = modifyRefreshWorkflow.getRefreshHandler();
        JobKey jobKey = JobKey.jobKey(handler.getWorkflowName(), handler.getWorkflowGroup());

        try {
            if (modifyRefreshWorkflow instanceof SuspendRefreshWorkflow) {
                checkExists(jobKey);
                quartzScheduler.pauseJob(jobKey);
            } else if (modifyRefreshWorkflow instanceof ResumeRefreshWorkflow) {
                checkExists(jobKey);
                Map<String, String> dynamicOptions =
                        ((ResumeRefreshWorkflow<?>) modifyRefreshWorkflow).getDynamicOptions();
                if (dynamicOptions.isEmpty()) {
                    quartzScheduler.resumeJob(jobKey);
                } else {
                    // quartz cannot update a job's data in place, so re-create it carrying the
                    // new dynamic options, reusing the existing cron expression
                    CronTrigger trigger =
                            (CronTrigger)
                                    quartzScheduler.getTrigger(
                                            TriggerKey.triggerKey(
                                                    jobKey.getName(), jobKey.getGroup()));
                    String cronExpression = trigger.getCronExpression();
                    quartzScheduler.deleteJob(jobKey);
                    scheduleWorkflow(jobKey.getName(), cronExpression, dynamicOptions);
                }
            } else {
                throw new WorkflowException(
                        String.format(
                                "Unsupported modify refresh workflow type %s.",
                                modifyRefreshWorkflow.getClass().getSimpleName()));
            }
        } catch (org.quartz.SchedulerException e) {
            throw new WorkflowException(
                    String.format("Failed to modify refresh workflow %s.", jobKey), e);
        }
    }

    @Override
    public void deleteRefreshWorkflow(
            DeleteRefreshWorkflow<InProcessRefreshHandler> deleteRefreshWorkflow)
            throws WorkflowException {
        InProcessRefreshHandler handler = deleteRefreshWorkflow.getRefreshHandler();
        JobKey jobKey = JobKey.jobKey(handler.getWorkflowName(), handler.getWorkflowGroup());
        try {
            quartzScheduler.deleteJob(jobKey);
            triggers.remove(handler.getWorkflowName());
        } catch (org.quartz.SchedulerException e) {
            throw new WorkflowException(
                    String.format("Failed to delete refresh workflow %s.", jobKey), e);
        }
    }

    private void scheduleWorkflow(
            String workflowName, String cronExpression, Map<String, String> dynamicOptions)
            throws org.quartz.SchedulerException, WorkflowException {
        JobKey jobKey = JobKey.jobKey(workflowName, WORKFLOW_GROUP);
        if (quartzScheduler.checkExists(jobKey)) {
            throw new WorkflowException(
                    String.format("Refresh workflow %s already exists.", jobKey));
        }

        JobDetail jobDetail =
                JobBuilder.newJob(InProcessRefreshJob.class).withIdentity(jobKey).build();
        jobDetail.getJobDataMap().put(SCHEDULER_ID, schedulerId);
        jobDetail.getJobDataMap().put(WORKFLOW_NAME, workflowName);
        dynamicOptions.forEach(
                (key, value) -> jobDetail.getJobDataMap().put(dynamicOptionKey(key), value));

        CronTrigger cronTrigger =
                newTrigger()
                        .withIdentity(TriggerKey.triggerKey(workflowName, WORKFLOW_GROUP))
                        .withSchedule(
                                cronSchedule(cronExpression)
                                        .withMisfireHandlingInstructionIgnoreMisfires())
                        .forJob(jobDetail)
                        .build();

        quartzScheduler.scheduleJob(jobDetail, cronTrigger);
        LOG.info(
                "Scheduled refresh workflow {} with cron expression {}.",
                workflowName,
                cronExpression);
    }

    private void checkExists(JobKey jobKey)
            throws org.quartz.SchedulerException, WorkflowException {
        if (!quartzScheduler.checkExists(jobKey)) {
            throw new WorkflowException(
                    String.format("Refresh workflow %s does not exist.", jobKey));
        }
    }

    private static String dynamicOptionKey(String key) {
        return "dynamicOption." + key;
    }

    @VisibleForTesting
    public Scheduler getQuartzScheduler() {
        return quartzScheduler;
    }

    /** The quartz {@link Job} that invokes the lifecycle directly, with no network hop. */
    public static class InProcessRefreshJob implements Job {

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            String schedulerId = context.getJobDetail().getJobDataMap().getString(SCHEDULER_ID);
            String workflowName = context.getJobDetail().getJobDataMap().getString(WORKFLOW_NAME);

            InProcessWorkflowScheduler scheduler = INSTANCES.get(schedulerId);
            if (scheduler == null) {
                throw new JobExecutionException(
                        String.format(
                                "Scheduler %s is gone; cannot refresh %s.",
                                schedulerId, workflowName));
            }
            PeriodicRefreshTrigger trigger = scheduler.triggers.get(workflowName);
            if (trigger == null) {
                throw new JobExecutionException(
                        String.format("No refresh action registered for %s.", workflowName));
            }

            Map<String, String> dynamicOptions = new java.util.HashMap<>();
            context.getJobDetail()
                    .getJobDataMap()
                    .forEach(
                            (key, value) -> {
                                if (key.startsWith("dynamicOption.")) {
                                    dynamicOptions.put(
                                            key.substring("dynamicOption.".length()),
                                            String.valueOf(value));
                                }
                            });

            String scheduleTime = formatScheduleTime(context.getScheduledFireTime());
            try {
                trigger.refresh(scheduleTime, dynamicOptions);
                LOG.info("Refreshed {} for schedule time {}.", workflowName, scheduleTime);
            } catch (Exception e) {
                LOG.error(
                        "Failed to refresh {} for schedule time {}.",
                        workflowName,
                        scheduleTime,
                        e);
                throw new JobExecutionException(e.getMessage(), e);
            }
        }

        private static String formatScheduleTime(Date fireTime) {
            return LocalDateTime.ofInstant(
                            Instant.ofEpochMilli(fireTime.getTime()), ZoneId.systemDefault())
                    .format(SCHEDULE_TIME_FORMATTER);
        }
    }
}
