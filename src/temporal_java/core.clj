(ns temporal-java.core
  (:import (io.temporal.workflow
            Workflow)
           (io.temporal.activity
            ActivityOptions)
           (java.time Duration))
  (:gen-class))

(defprotocol GreetingWorkflow
  "This is the method that is executed when the Workflow Execution is started.
   The Workflow Execution completes when this method finishes execution."
  (get-greeting [this name]))

(defprotocol GreetingActivities
  "Define your activity method which can be called during workflow execution"
  (compose-greeting [this greeting name]))

(defn make-greetingworkflow-impl []
  (reify
    Workflow
    (getGreeting [this name]
      (let [activities (Workflow/newActivityStub
                        GreetingActivities
                        (-> (ActivityOptions/newBuilder)
                            (.setStartToCloseTimeout (Duration/ofSeconds 2))
                            .build))]
        (compose-greeting activities "Hello" name)))))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
