(ns thdr.kfk.avro-bridge.core-test
  (:require [clojure.test :refer :all]
            [cheshire.core :as json]
            [thdr.kfk.avro-bridge.core :refer [->java ->clj] :as core]
            [camel-snake-kebab.core :as csk])
  (:import [org.apache.avro Schema]))

(defn- make-schema [m]
  (json/generate-string m))

(defn- roundtrip [schema data opts]
  (-> schema (->java data opts) (->clj opts)))

(defn- test-roundtrip [schema data & [opts]]
  (let [schema (Schema/parse (make-schema schema))
        result (roundtrip schema data opts)]
    (if (#'core/bytes? data)
      (is (= (seq data) (seq result)))
      (is (= data result)))))

(deftest avro-bridge-roundtrip-test
  (test-roundtrip "null" nil)
  (test-roundtrip "string" "test")
  (test-roundtrip "int" 1)
  (test-roundtrip "long" 1)
  (test-roundtrip "float" 1.0)
  (test-roundtrip "double" 1.0)
  (test-roundtrip "bytes" (bytes (byte-array 0)))
  (test-roundtrip {:type "array" :items "int"} [1 2])
  (test-roundtrip {:type "array" :items "string"} ["1" "2"])
  (test-roundtrip {:type "map" :values "string"} {:a "1" :b "2"})
  (test-roundtrip {:type   "record"
                   :name   "Test"
                   :fields [{:name "a" :type "string"}
                            {:name "b" :type "int"}]}
                  {:a "1" :b 2})
  (test-roundtrip ["null" "string"] nil)
  (test-roundtrip ["null" "string"] "test")
  (test-roundtrip {:name "test" :type "enum" :symbols ["TEST" "ME"]} :TEST)
  (test-roundtrip {:name "test" :type "enum" :symbols ["TEST" "ME"]} :ME)
  (test-roundtrip {:name "uuid" :type "fixed" :size 36}
                  (byte-array (map byte (str (java.util.UUID/randomUUID))))))

(deftest test-defaults
  (let [schema (Schema/parse (make-schema {:type "record"
                                           :name "Test"
                                           :fields [{:name "a" :type "string" :default "hi"}
                                                    {:name "b" :type "int"}]}))]
    (testing "default is used when no key is provided"
      (is (= {:a "hi" :b 0} (roundtrip schema {:b 0} {}))))
    (testing "defaults is not used when a valid valiue is provided"
      (is (= {:a "value" :b 0} (roundtrip schema {:a "value" :b 0} {}))))))

(deftest casing-fields

  (test-roundtrip {:type   "record"
                   :name   "DefaultSnakeCase"
                   :fields [{:name "a_snake_case" :type "string"}
                            {:name "b" :type "int"}]}
                  {:a-snake-case "1" :b 2})

  (test-roundtrip {:type   "record"
                   :name   "CamelCase"
                   :fields [{:name "aCamelCasedField" :type "string"}]}
                  {:a-camel-cased-field "any value"}
                  {:java-field-fn (comp name csk/->camelCase)})

  (test-roundtrip {:type   "record"
                   :name   "CamelCaseInnerRecord"
                   :fields [{:type {:type   "record"
                                    :name   "InnerRecord"
                                    :fields [{:name "aCamelCasedField" :type "string"}]}
                             :name "innerRecord"}]}
                  {:inner-record {:a-camel-cased-field "any value"}}
                  {:java-field-fn (comp name csk/->camelCase)})

  (test-roundtrip {:type   "record"
                   :name   "DoNothingWithTheKeys"
                   :fields [{:name "aCamelCasedField" :type "string"}]}
                  {"aCamelCasedField" "any value"}
                  {:java-field-fn identity
                   :clj-field-fn  identity})

  (test-roundtrip {:type "map" :values "string"}
                  {:a_snake "1" :b "2"}
                  {:clj-field-fn (comp keyword csk/->snake_case)}))

(deftest optionally-ignore-unkown-fields

  (let [schema (Schema/parse (make-schema {:type   "record"
                                           :name   "DefaultSnakeCase"
                                           :fields [{:name "just_this_one" :type "string"}]}))]
    (is (thrown? Exception
                 (->java schema
                         {:just-this-one            "1"
                          :but-this-is-an-extra-one "2"})))

    (is (= {:just-this-one "1"}
           (->clj
             (->java schema
                     {:just-this-one            "1"
                      :but-this-is-an-extra-one "2"}
                     {:ignore-unknown-fields? true}))))))

(deftest exceptions-are-useful
  (testing "Contains path to error"
    (let [schema (Schema/parse (make-schema {:type   "record"
                                             :name   "exceptional"
                                             :fields [{:name "a"
                                                       :type {:type  "array"
                                                              :items {:type "map" :values "string"}}}]}))]
      (is (thrown-with-msg?
            Exception
            #"\[:a 1 :c\]"
            (->java schema
                    {:a [{:b "ok"}
                         {:b "ok" :c 1}]})))

      (is (thrown-with-msg?
            Exception
            #"\[\]"
            (->java schema
                    "not a record")))

      (is (thrown-with-msg?
            Exception
            #"\[:a\]"
            (->java schema
                    {:a "not a list"})))

      (is (thrown-with-msg?
            Exception
            #"\[:a 1\]"
            (->java schema
                    {:a [{:b "ok"}
                         "not a map"]}))))))
