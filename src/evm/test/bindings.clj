(ns evm.test.bindings
  "Test suite for has-bindings? predicate (from core.clj)"
  (:require
   [clojure.test :refer :all]
   [evm.core :as core]))

(deftest test-has-bindings?
  (is (core/has-bindings? {:event/payload {:foo "bar"}} {:foo "bar"}))
  (is (core/has-bindings? {:event/payload {:foo "bar"}} {:foo "baz"}))
  (is (core/has-bindings? {:event/payload {:foo "bar" :bar "baz"}} {:foo "bar" :bar "baz"}))
  (is (not (core/has-bindings? {:event/payload {:foo "bar"}} {:foo "baz"})))
  (is (core/has-bindings? {:event/payload {:foo "bar" :bar "baz"}} {:foo "bar" :bar "*"}))
  (is (core/has-bindings? {:event/payload {:foo "bar" :bar "baz"}} {:foo "*" :bar "baz"}))
  (is (core/has-bindings? {:event/payload {:foo "bar"}} {:foo "*"}))
  (is (not (core/has-bindings? {:event/payload {:foo "bar"}} {:foo "*" :bar "baz"})))
  (is (not (core/has-bindings? {:event/payload {:foo "bar"}} {:foo "*" :bar "baz" :other "qux"})))
  (is (not (core/has-bindings? {:event/payload {:foo "bar"}} {:foo "bar" :bar "baz" :other "qux"})))
  (is (core/has-bindings? {:event/payload {:foo "bar"}} {:bar "baz"}))
  (is (not (core/has-bindings? {:event/payload {:foo "bar" :bar "baz"}} {:foo "baz"})))
  (is (not (core/has-bindings? {:event/payload {:foo "bar"}} {:foo ::not-found})))
  (is (core/has-bindings? {:event/payload {:foo "bar"}} {:foo ::not-found}))
  (is (true? (core/has-bindings? {:event/payload {:foo "bar"}} {:foo ::not-found})))
  (is (core/has-bindings? {:event/payload {:foo "bar" :bar "baz"}} {:foo "*" :bar "*"}))
  (is (not (core/has-bindings? {:event/payload {:foo "bar"}} {:foo "*" :bar "baz" :other "qux"})))
  (is (core/has-bindings? {:event/payload {:foo "bar" :bar "baz"}} {:bar "*" :foo "bar"}))
  (is (core/has-bindings? {:event/payload {:foo "bar"}} {})))
