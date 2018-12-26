(ns com.joshuadavey.db-introspector.pg
  (:require [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [clojure.set :as set])
  (:import [org.postgresql.jdbc PgArray]))

(def columns-sql (slurp (io/resource "com/joshuadavey/db_introspector/columns.sql")))

(defn- dasherize [s]
  (str/replace s #"_" "-"))

(def generic-data-types
  {"bigint"                      :long
   "array"                       :array
   "boolean"                     :boolean
   "character varying"           :string
   "character"                   :char
   "citext"                      :string
   "date"                        :date
   "decimal"                     :decimal
   "double precision"            :double
   "enum"                        :enum
   "integer"                     :int
   "json"                        :json
   "jsonb"                       :json
   "numeric"                     :decimal
   "public.citext"               :string
   "real"                        :float
   "smallint"                    :int
   "text"                        :string
   "timestamp with time zone"    :instant
   "timestamp without time zone" :instant
   "uuid"                        :uuid})

(defn- normalize-column-keys-and-vecs [m]
  (persistent!
   (reduce-kv (fn [a k v]
                (assoc! a
                        (keyword (dasherize (name k)))
                        (if (instance? PgArray v)
                          (vec (.getArray ^PgArray v))
                          v)))
              (-> m empty transient)
              m)))

(defn- column-row-fn
  [elide-schemas]
  (let [elide-schema-pattern (re-pattern (str "^(" (str/join "|" elide-schemas) ")\\."))]
    (fn [col]
      (let [table-schema (-> col :table_schema dasherize)
            table-name (-> col :table_name dasherize)
            table (if (contains? elide-schemas table-schema)
                    table-name
                    (str table-schema "." table-name))
            column (keyword table (-> col :column_name dasherize))
            type (get generic-data-types (:base_data_type col)
                      (-> col :data_type (str/replace elide-schema-pattern "") dasherize keyword))
            full-column-name (str/replace (:fullcolname col) elide-schema-pattern "")]
        (-> col
            normalize-column-keys-and-vecs
            (assoc :table (keyword table)
                   :column column
                   :full-column-name full-column-name
                   :type type))))))

(defn read-raw-pg-column-info
  ([conn]
   (read-raw-column-info conn {:elide-schemas #{"public"}}))
  ([conn {:keys [elide-schemas] :as opts}]
   (jdbc/query conn columns-sql
               {:row-fn (column-row-fn (set elide-schemas))})))

(defn read-column-info
  "Given a cloure.java.jdbc postgres connection, returns a vector of
  column information, as maps. Each map will contain information about
  one column."
  ([conn]
   (read-column-info conn {:elide-schemas #{"public"}}))
  ([conn opts]
   (let [raw (read-raw-pg-column-info conn opts)
         colid->column (into {} (map (juxt :colid :column)) raw)
         to-column-name (comp colid->column vec)]
     (into []
           (map (fn [info]
                  {:table (:table info)
                   :column (:column info)
                   :pkey (not-empty (mapv to-column-name (:pkey info)))
                   :type (:type info)
                   :db (select-keys info [:table-schema
                                          :table-name
                                          :column-name
                                          :index-names
                                          :full-column-name
                                          :base-data-type
                                          :data-type
                                          :column-default])
                   :nullable? (:is-nullable info)
                   :dependencies (not-empty (mapv (fn [cols] (mapv to-column-name cols)) (:dependencies info)))
                   :domain (case (:type info)
                             (:string :char) {:max-length (:character-maximum-length info)}
                             :decimal {:precision (:numeric-precision info)
                                       :scale (:numeric-scale info)}
                             :enum {:allowed-values (:allowed info)}

                             nil)}))
           raw))))
