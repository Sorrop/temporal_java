(ns temporal-java.polyglot
  (:import [io.temporal.activity ActivityInterface ActivityMethod ActivityOptions]
           [io.temporal.client WorkflowClient WorkflowOptions]
           [io.temporal.serviceclient WorkflowServiceStubs]
           [io.temporal.worker Worker WorkerFactory]
           [io.temporal.workflow Workflow WorkflowInterface WorkflowMethod]
           [java.time Duration]))

;; Workflow interface
(definterface
    ^{io.temporal.workflow.WorkflowInterface true}
    PolyglotWorkflow
  (^{io.temporal.workflow.WorkflowMethod true} exec []))


;; Workflow implementation
(defn polyglot-workflow-impl []
  (reify PolyglotWorkflow
    (exec [this]
      (let [options         (-> (ActivityOptions/newBuilder)
                                (.setStartToCloseTimeout (Duration/ofSeconds 3))
                                (.setTaskQueue "simple-queue-python")
                                .buid)
            python-activity (Workflow/newUntypedActivityStub options)]
        (.execute python-activity "python_activity" java.lang.String "ClojureWorkflow")))))

;; Activity interface
(definterface
    ^{io.temporal.activity.ActivityInterface true}
    HelloActivity
  (^{io.temporal.activity.ActivityMethod true} sayHello [name]))

;; Define the GreetingActivities implementation
(defn hello-activity-impl []
  (reify HelloActivity
    (sayHello [this name]
      (str "Hello " name "!"))))


(defn start-workflow []
  (let [service (WorkflowServiceStubs/newLocalServiceStubs)
        ;; Get a Workflow service client
        client (WorkflowClient/newInstance service)
        ;; Create python workflow options
        py-wf-opts (-> (WorkflowOptions/newBuilder)
                       (.setTaskQueue "simple-queue-clojure")
                       (.setWorkflowId "SimpleWorkflowClojure")
                       .build)
        ;; Create workflow client stub
        workflow (-> client
                     (.newWorkflowStub PolyglotWorkflow py-wf-opts))]
    (.exec workflow)))


(defn run-worker []
  (let [service (WorkflowServiceStubs/newLocalServiceStubs)
        client (WorkflowClient/newInstance service)
        factory (WorkerFactory/newInstance client)
        worker (.newWorker factory "simple-queue-clojure")
        ;; Register workflow implementation with the worker
        _  (.registerWorkflowImplementationTypes worker (into-array java.lang.Class [(class (polyglot-workflow-impl))]))
        _ (.registerActivitiesImplementations worker (into-array Object [(hello-activity-impl)]))
        ]
    (.start factory)))
