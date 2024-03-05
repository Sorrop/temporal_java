(ns temporal-java.first-try
  (:import [io.temporal.activity ActivityInterface ActivityMethod ActivityOptions]
           [io.temporal.client WorkflowClient
            WorkflowClientOptions
            WorkflowOptions
            WorkflowStub]
           [io.temporal.client.schedules
            Schedule
            ScheduleOptions
            ScheduleActionStartWorkflow
            ScheduleSpec
            ScheduleSpec$Builder
            ScheduleClient
            ScheduleClientOptions
            ScheduleState
            ScheduleUpdate]
           [io.temporal.serviceclient WorkflowServiceStubs]
           [io.temporal.worker Worker WorkerFactory]
           [io.temporal.workflow Workflow WorkflowInterface WorkflowMethod Functions]
           [io.temporal.common.converter
            PayloadConverter
            NullPayloadConverter
            ByteArrayPayloadConverter
            ProtobufPayloadConverter
            ProtobufJsonPayloadConverter
            JacksonJsonPayloadConverter
            DefaultDataConverter]
           [java.time Duration]
           [io.temporal.failure ApplicationFailure]
           [java.util
            Collections])
  (:require [jsonista.core :as json]))

(def task-queue "HelloActivityTaskQueue")

(def workflow-id "HelloActivityWorkflow")


;; Workflow interface
(definterface
    ^{io.temporal.workflow.WorkflowInterface true}
    GreetingWorkflow
  (^{io.temporal.workflow.WorkflowMethod true} getGreeting [name]))

;; Activity interface
(definterface
    ^{io.temporal.activity.ActivityInterface true}
    GreetingActivities
  (^{io.temporal.activity.ActivityMethod {:name "greet"}} composeGreeting [greeting name]))

;; Define the workflow implementation
(defn greeting-workflow-impl []
  (reify GreetingWorkflow
    (getGreeting [this name]
      ;; Define the GreetingActivities stub
      (let [activities (Workflow/newActivityStub
                        GreetingActivities
                        (-> (ActivityOptions/newBuilder)
                            (.setStartToCloseTimeout (Duration/ofSeconds 2))
                            .build))]
        ;; This is a blocking call that returns only after the activity has completed
        (.composeGreeting activities "Hello" name)))))


;; Define the GreetingActivities implementation
(defn greeting-activities-impl []
  (reify GreetingActivities
    (composeGreeting [this greeting name]
      (str greeting " " name "!"))))

;; Create the WorkflowServiceStubs, WorkflowClient, WorkerFactory, and Worker objects separately,
;; and register the workflow and activities.

(defn run []
  ;; Get a Workflow service stub
  (let [service (WorkflowServiceStubs/newLocalServiceStubs)
        ;; Get a Workflow service client
        client (WorkflowClient/newInstance service)
        ;; Define the workflow factory
        factory (WorkerFactory/newInstance client)
        ;; Define the workflow worker
        worker (.newWorker factory task-queue)
        ;; Register workflow implementation with the worker
        _  (.registerWorkflowImplementationTypes worker (into-array java.lang.Class [(class (greeting-workflow-impl))]))
        ;; Register activity implementation with the worker
        _ (.registerActivitiesImplementations worker (into-array Object [(greeting-activities-impl)]))
        ;; Start all registered workers
        _ (.start factory)
        ;; Create the workflow client stub
        workflow (-> client
                     (.newWorkflowStub GreetingWorkflow
                                       (-> (WorkflowOptions/newBuilder)
                                           (.setWorkflowId workflow-id)
                                           (.setTaskQueue task-queue)
                                           (.build))))
        ;; Execute the workflow and wait for completion
        greeting (.getGreeting workflow "World")]
    ;; Display workflow execution results
    (println greeting)))


;; ====================================================================
;; ====================================================================
;; ====================================================================

(defn ex->ApplicationFailure [exception type]
  (let [msg (.getMessage exception)
        stack-trace (.getStackTrace exception)
        details     (into-array Object [stack-trace])]
    (ApplicationFailure/newNonRetryableFailure msg type details)))

(defn greeting-workflow-impl-no-activity []
  (reify GreetingWorkflow
    (getGreeting [this name]
      #_(str "Hello " name "!!!!!")
      (try
        name
        #_(/ 0)
        (catch Exception e
          (throw (ex->ApplicationFailure e "some-failure")))))))

(def converter
  (DefaultDataConverter.
   (into-array PayloadConverter
               [(NullPayloadConverter.)
                (ByteArrayPayloadConverter.)
                (ProtobufJsonPayloadConverter.)
                (ProtobufPayloadConverter.)
                (JacksonJsonPayloadConverter. json/keyword-keys-object-mapper)])))


;; The following function receives a task-queue from which to poll
;; and a workflow implementation that will use to execute a task
;; that picks from that queue
(defn start-worker [task-queue workflow-impl]
  (let [service (WorkflowServiceStubs/newLocalServiceStubs)
        client-opts (-> (WorkflowClientOptions/newBuilder)
                        (.setDataConverter converter)
                        (.build))
        client (WorkflowClient/newInstance service client-opts)
        factory (WorkerFactory/newInstance client)
        worker (.newWorker factory task-queue)]
    (.registerWorkflowImplementationTypes worker (into-array java.lang.Class [(class (workflow-impl))]))
    (.start factory)))

;; The following function will order the execution of a workflow with a specific
;; input by putting the task into the specified task-queue
;; This function can be executed in an entirely different process (or machine)
;; from the `start-worker` that actually executes the task
(defn start-workflow [{:keys [workflow-id task-queue input workflow-interface]}]
  (let [service (WorkflowServiceStubs/newLocalServiceStubs)
        client-opts (-> (WorkflowClientOptions/newBuilder)
                        (.setDataConverter converter)
                        (.build))
        client (WorkflowClient/newInstance service client-opts)
        options (-> (WorkflowOptions/newBuilder)
                    (.setWorkflowId workflow-id)
                    (.setTaskQueue task-queue)
                    (.build))
        workflow (.newWorkflowStub client workflow-interface options)
        ;; the following call will block
        result (.getGreeting workflow input)

        workflow-id (-> (WorkflowStub/fromTyped workflow)
                        .getExecution
                        .getWorkflowId)]
    {:workflow-id  workflow-id
     :result       result}))


(defn temporal-client []
  (let [service (WorkflowServiceStubs/newLocalServiceStubs)
        client-opts (-> (WorkflowClientOptions/newBuilder)
                        (.setDataConverter converter)
                        (.build))]
    (WorkflowClient/newInstance service client-opts)))

(defn schedule-client []
  (let [service (WorkflowServiceStubs/newLocalServiceStubs)
        client-opts (-> (ScheduleClientOptions/newBuilder)
                        (.setDataConverter converter)
                        (.build))]
    (ScheduleClient/newInstance service client-opts)))

(defn create-schedule
  [{:keys [workflow-id
           task-queue
           input
           workflow-interface
           cron
           timezone]
    :or   {timezone "UTC"}}]

  (println timezone)
  (let [workflow-opts (-> (WorkflowOptions/newBuilder)
                          (.setWorkflowId workflow-id)
                          (.setTaskQueue task-queue)
                          (.build))
        action (-> (ScheduleActionStartWorkflow/newBuilder)
                   (.setWorkflowType workflow-interface)
                   (.setArguments (into-array Object [input]))
                   (.setOptions workflow-opts)
                   (.build))
        schedule-spec (-> (ScheduleSpec/newBuilder)
                          (.setCronExpressions (Collections/singletonList cron))
                          (.setTimeZoneName​ timezone)
                          .build)]
    (-> (Schedule/newBuilder)
        (.setAction action)
        (.setSpec schedule-spec)
        (.build))))

(defn schedule-workflow
  [{:keys [schedule-client
           schedule-id
           workflow-id
           task-queue
           input
           workflow-interface
           cron
           timezone]
    :as params}]
  (let [schedule (create-schedule params)
        schedule-opts (-> (ScheduleOptions/newBuilder)
                          (.build))]
    (.createSchedule schedule-client schedule-id schedule schedule-opts)))


(defn get-schedule-handle
  [{:keys [schedule-client schedule-id]}]
  (.getHandle schedule-client schedule-id))

(defn delete-scheduled-workflow
  [{:keys [schedule-client schedule-id] :as params}]
  (let [handle (get-schedule-handle params)]
    (.delete handle)))

(defn describe-scheduled-workflow [{:keys [schedule-client schedule-id] :as params}]
  (let [handle (get-schedule-handle params)]
    (.describe handle)))

(defn update-schedule [{:keys [schedule-client schedule-id cron timezone] :as params}]
  (let [handle (get-schedule-handle params)]
    (.update handle
             (reify
               io.temporal.workflow.Functions$Func1
               (apply [this input]
                 (let [schedule-spec (-> (ScheduleSpec/newBuilder)
                                         (.setCronExpressions (Collections/singletonList cron))
                                         (.setTimeZoneName​ timezone)
                                         (.build))
                       builder (-> (Schedule/newBuilder
                                    (-> (.getDescription input)
                                        .getSchedule))
                                   (.setSpec schedule-spec))
                       state (-> (ScheduleState/newBuilder)
                                 .build)]
                   (.setState builder state)
                   (ScheduleUpdate. (.build builder))))))))

(comment

  ;; this will block until the workflow is executed and returns its result
  (try (start-workflow {:workflow-id        "my-new-workflow"
                        :task-queue         "some-queue"
                        :input              {:data "Mr Dumbdumb"}
                        :workflow-interface GreetingWorkflow})
       (catch Exception e
         e))

  ;; in another process, execute
  (start-worker "my-task-queue" greeting-workflow-impl-no-activity)

  (let [client (schedule-client)
        args   {:schedule-client    client
                :schedule-id        "my-schedule-id"
                :workflow-id        "my-workflow-id"
                :task-queue         "my-task-queue"
                :input              {:a 34}
                :workflow-interface GreetingWorkflow
                :cron               "*/2 * * * *"
                :timezone           "Europe/Berlin"}]
    (schedule-workflow args))

  (let [client (schedule-client)]
    (update-schedule {:schedule-client client
                      :schedule-id "my-schedule-id"
                      :cron "*/4 * * * *"})))


