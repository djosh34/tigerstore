(ns offline.versioned-register
  "Exact VersionedRegister record from jepsen-io/etcd b47cc401f27bfb7c28afd9ac088b5f0dca4501b8."
  (:require [knossos.model :as model])
  (:import (knossos.model Model)))

(defrecord VersionedRegister [version value]
  Object
  (toString [this] (str "v" version ": " value))

  Model
  (step [model op]
    (let [[op-version op-value] (:value op)
          version' (inc version)]
      (condp = (:f op)
        :write (if (and (not (nil? op-version))
                        (not= version' op-version))
                 (model/inconsistent
                   (str "can't go from version " version " to " op-version))
                 (VersionedRegister. version' op-value))

        :cas   (let [[v v'] op-value]
                 (cond (and (not (nil? op-version))
                            (not= version' op-version))
                       (model/inconsistent
                         (str "can't go from version " version " to "
                              op-version))

                       (not= value v)
                       (model/inconsistent (str "can't CAS " value " from " v
                                                " to " v'))

                       true
                       (VersionedRegister. version' v')))

        :read (cond (and (not (nil? op-version))
                         (not= version op-version))
                    (model/inconsistent
                      (str "can't read version " op-version " from version "
                           version))

                    (and (not (nil? op-value))
                         (not= value op-value))
                    (model/inconsistent
                      (str "can't read " op-value " from register " value))

                    true
                    model)))))

