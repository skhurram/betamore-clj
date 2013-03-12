(ns betamore-clj.core
  (:require [clojure.pprint :refer [pprint]]
            [clojure.java.io :as io])
  (:import [org.jsoup Jsoup]))

(def start "http://en.wikipedia.org/wiki/America")

(defn map->url
  [{:keys [protocol host port file]}]
  (new java.net.URL protocol host (int port) file))

(defn url->map
  [url]
  (let [u (java.net.URL. (str url))]
    {:protocol (.getProtocol u)
     :host (.getHost u)
     :port (.getPort u)
     :file (.getFile u)}))

;; (map->url (url->map start))
;;   #<URL http://en.wikipedia.org/wiki/Quail>

;;Document Jsoup/parse(String html)
(defn parse
  ([] (parse start))
  ([source] (Jsoup/parse source)))

;;(links (parse))

(defn get-link-attribute
  [elt]
  (.attr elt "href"))

;;; returns Elements, which extends List
(defn links
  [doc]
  (.select doc "a[href]"))

(defn wiki-link?
  [link]
  (and link
       (.startsWith link "/wiki")
       (not (.contains link ":"))))

;; (pprint (filter wiki-link? (map get-link-attribute (links (parse)))))

(comment
  "/wiki/Quail_(disambiguation)"
  "/wiki/File:Brown_Quail.jpg"
  "/wiki/Brown_Quail"
  "/wiki/Biological_classification"
  "/wiki/Animal"
  ...
  "/wiki/File:The_Childrens_Museum_of_Indianapolis_-_Quail_trap.jpg"
  "/wiki/File:The_Childrens_Museum_of_Indianapolis_-_Quail_trap.jpg"
  "/wiki/Mountain_Quail"
  ...
   "/wiki/Help:Categories"
   "/wiki/Category:Quails"
   "/wiki/Category:Domesticated_birds"
   "/wiki/Category:Articles_with_%27species%27_microformats"
   )

;;; Kill things with colons in them? Naaa.


(defn urls-to-download
  [doc]
  (let [links (->> (links doc)
                   (map get-link-attribute)
                   (filter wiki-link?))
        starting-url (url->map start)]
    (map (comp
          #(map->url %)
          #(assoc starting-url :file %)) links)))

(comment
;; (pprint (urls-to-download (parse)))
"#<URL http://en.wikipedia.org/wiki/Quail_(disambiguation)>
#<URL http://en.wikipedia.org/wiki/File:Brown_Quail.jpg>
#<URL http://en.wikipedia.org/wiki/Brown_Quail>
#<URL http://en.wikipedia.org/wiki/Biological_classification>
#<URL http://en.wikipedia.org/wiki/Animal>
#<URL http://en.wikipedia.org/wiki/Chordate>
#<URL http://en.wikipedia.org/wiki/Bird>
#<URL http://en.wikipedia.org/wiki/Galliformes>
..."
)

;;; From http://clj-me.cgrand.net/2010/04/02/pipe-dreams-are-not-necessarily-made-of-promises/
(defn pipe []
  (let [q (java.util.concurrent.LinkedBlockingQueue.)
        EOQ (Object.)
        NIL (Object.)
        the-seq (fn s [] (lazy-seq (let [x (.take q)]
                                    (when-not (= EOQ x)
                                      (cons (when-not (= NIL x) x) (s))))))]
    [(the-seq)
     (fn [x] (.put q (or x NIL)))
     ;; Throw EOQ
     EOQ]))

(defn write-file
  [path content]
  (let [relative-path (:file (url->map path))
        abs-path (str "downloaded/" relative-path ".html")]
    (io/make-parents abs-path)
    (spit (io/file abs-path) content)))

(defn go
  []
  (let [total-count (atom 1)
        [q enqueue _] (pipe)]
    (enqueue start)
    (loop [q q]
      (if-not (> @total-count 100)
        (let [f (first q)
              content (slurp f)
              next-urls (urls-to-download (parse content))]
          (println f)
          (write-file f content)
          (doseq [n next-urls]
            (enqueue n))
          (swap! total-count inc)
          (recur (next q)))))))


(defn do-concurrent
  [worker url enqueue count]
  (send-off
   worker
   (fn [_]
     (let [content (slurp url)
           next-urls (urls-to-download (parse content))]
       (println url)
       (write-file url content)
       (doseq [n next-urls]
         (enqueue n))
       (swap! count inc)))))

(defn poll-test
  "Helpful for async processes, tests if a condition
   happens within the specified interval, starting now."
  [test-fn interval-ms timeout]
  (let [now #(.getTime (java.util.Date.))
        start-time (now)
        end-time (+ start-time timeout)]
    (loop []
      (if-let [val (test-fn)]
        val
        (when (< (now) end-time)
          (Thread/sleep interval-ms)
          (recur))))))

(defn go-concurrent
  "Uses agents as workers, depleting a queue to task them.  Blocks until they're all done."
  [n-workers]
  (let [limit 1000
        total-count (atom 0)
        [q enqueue eoq] (pipe)
        workers (cycle (take n-workers (repeatedly #(agent nil))))
        seen-urls (atom #{})
        completed-count (atom 0)]
    (enqueue start)
    (loop [workers workers
           q q]
      (if (< @total-count limit)
        (when-let [[q1] (seq q)]
          (if-not (@seen-urls q1)
            (do (do-concurrent (first workers) q1 enqueue completed-count)
                (swap! seen-urls conj q1)
                (swap! total-count inc)
                (recur (next workers) (next q)))
            (recur workers (next q))))))
    (poll-test #(>= @completed-count limit) 10 60000)
    (println "Completed" @completed-count)))