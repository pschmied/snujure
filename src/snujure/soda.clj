(ns snujure.soda
  (:use [snujure.lpass])
  (:import (com.socrata.api SodaImporter Soda2Producer)
           (com.socrata.model.importer Metadata)
           (com.socrata.exceptions DoesNotExistException MalformedQueryError))
  (:gen-class))


(defn make-importer
  "Creates an Socrata importer object. Takes a string of the domain;
  returns an importer."
  [domain]
  (SodaImporter/newImporter
   domain
   (lpass-get-user)
   (lpass-get-pass)
   (lpass-get-token)))

(defn make-working-copy
  "Creates a working copy of a dataset; takes an importer and a
  datasetid returns a working copy view"
  [importer datasetid]
  (.createWorkingCopy importer datasetid))

(defn make-metadata
  "Takes a map (2 levels deep only; keys must be strings), returns a
  Socrata metadata object"
  [m]
  (new Metadata m nil nil nil nil))

(defn get-dataset-view
  "Loads dataset info (i.e. a soda-java DatasetInfo) for a particular
  dataset"
  [importer datasetid]
  (.loadDatasetInfo importer datasetid))

(defn publish-dataset
  "Takes a domain and a dataset. Publishes it."
  [importer datasetid]
  (try (not (nil?  (.publish importer datasetid)))
       (catch MalformedQueryError e true)
       (catch DoesNotExistException e false)))

(defn get-metadata
  "Retrieves metadata object of a given dataset id. Returns a Metadata object."
  [importer datasetid]
  (let [dataset-info (.loadDatasetInfo importer datasetid)]
    (.getMetadata dataset-info)))

(defn get-custom-metadata
  "Retrieves custom metadata from a given domain / dataset"
  [importer datasetid]
  (let [metaobj (get-metadata importer datasetid)]
    (try (into {} (.getCustom_fields metaobj))
         (catch Exception e {}))))

(defn set-name-metadata
  "Updates the name of a dataset identified by its 4x4 dataset id
  on the specified domain with the contents of the supplied string"
  [importer dataset-view namestring]
  (.setName dataset-view namestring)
  (.updateDatasetInfo importer dataset-view))

(defn set-license-metadata
  "Updates the license of a dataset identified by its 4x4 dataset id
  on the specified domain with the license identifier"
  [importer dataset-view licenseid]
  (.setLicenseId dataset-view licenseid)
  (.updateDatasetInfo importer dataset-view))

(defn set-attribution-metadata
  "Updates the attribution of a dataset identified by its 4x4 dataset
  id on the specified domain with the license identifier"
  [importer dataset-view attribution]
  (.setAttribution dataset-view attribution)
  (.updateDatasetInfo importer dataset-view))

(defn set-tags-metadata
  "Updates the tags of a dataset given a list of vector of tag
  strings"
  [importer dataset-view tags-list]
  (.setTags dataset-view tags-list)
  (.updateDatasetInfo importer dataset-view))

(defn set-description-metadata
  "Updates the description of a dataset identified by its 4x4 dataset
  id on the specified domain with the contents of the supplied string"
  [importer dataset-view namestring]
  (.setDescription dataset-view namestring)
  (.updateDatasetInfo importer dataset-view))

(defn set-custom-metadata
  "Takes a map of metadata (2 levels deep only; keys must be strings),
  sets custom metadata for the specified Socrata dataset on the
  specified domain."
  [importer dataset-view metadata-map]
  (let [metaobj (get-metadata importer (.getId dataset-view))]
    (.setCustom_fields metaobj metadata-map)
    (.setMetadata dataset-view metaobj)
    (.updateDatasetInfo importer dataset-view)))

(defn add-custom-metadata
  "Takes a map of metadata (2 levels deep only; keys must be strings),
  adds metadata to the specified Socrata dataset on the specified
  domain."
  [importer dataset-view metadata-map]
  (let [old (get-custom-metadata importer (.getId dataset-view))
        merged (merge old map)]
    (set-custom-metadata importer dataset-view merged)))


(defn dataset-on-domain?
  "Takes a dataset 4x4 id and a domain, returns true if said 4x4
  exists on the domain, otherwise false"
  [domain datasetid]
  ;; This approach results in fetching metadata more often than we
  ;; strictly need, but it's also not too expensive of an operation
  (let [importer (make-importer domain)]
    (try
      (map? (get-custom-metadata importer datasetid))
      (catch DoesNotExistException e false))))

(defn which-domain-has-dataset?
  "Takes a dataset id and vector of domains, returns the domain that
  has the dataset in question."
  [datasetid domains]
  (let [resultset (filter #(dataset-on-domain? %1 datasetid) domains)]
    ; Assume / ensure one result. Other, bigger problems if not true.
    (first resultset)))
