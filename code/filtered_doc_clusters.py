import pickle
import numpy as np
import nltk
import re
import langid
import boto
from boto.s3.key import Key
import gzip
from nltk.corpus import stopwords
from collections import Counter
import sys
import tempfile
from scipy import spatial
from pyspark.mllib.feature import IDF
from pyspark.mllib.clustering import KMeans, KMeansModel
import random
from random import randint
from pyspark.mllib.linalg import SparseVector
import operator
from preparing_cursor import PreparingCursor
from crawl_data import CrawlData
from pyspark import SparkContext, SparkConf
import ConfigParser
import time



def detect_lang(doc):
    """Infer a language given a document

    Input: Document string

    Output: ISO 639-1 language code

    Examples:
        >>> print(detect_lang("today is a good day."))
        en
    """
    lang_id = 'en'
    if len(doc) > 1400 :
        lang_id = langid.classify(doc[400:1400])[0]
    else:
        lang_id = langid.classify(doc[0:])[0]
    if lang_id in langdict:
        langaccum[langdict[lang_id]].add(1)
    return lang_id

def tokenize_text(text):
    """Tokenize text into list of words

    Input: Text string

    Globals: Relies on chahced stopwords, regex and stemmer objects for speed optimizations
    stopwordsCache = set(stopwords.words('english'))
    regex = re.compile("[^a-zA-Z]")
    stemmer = SnowballStemmer("english")

    Output: List of tokenized words

    Examples:
        >>> print(tokenize_text("today is a good day."))
        [u'today', u'good', u'day']
    """
    clean_txt = regex.sub(' ', text)
    clean_txt = clean_txt.lower()
    words = clean_txt.split()
    #stemming is computationally very expensive
    #clean_words = [stemmer.stem(word) for word in words if word not in stopwordsCache and len(word) > 2]
    clean_words = [word for word in words if word not in stopwordsCache and len(word) > 2]
    return clean_words


def get_word_vec(text, vocabulary, vocablen):
    """Converts text into sparse vector representation

    Input: Text string, list of vocabulary words, length of vocabulary

    Output: Sparse vector representation of the list

    Examples:
        >>> print(tokenize_text("today is a good day."))
        SparseVector(9974, {74: 0.5774, 92: 0.5774, 765: 0.5774, 1413: 0.5774, 2327: 0.5774, 3704: 0.5774, 5738: 0.5774})
    """
    word_lst = tokenize_text(text)
    word_counter = Counter(word_lst)
    norm = np.linalg.norm(word_counter.values())
    data = {}
    for i, v in enumerate(vocabulary):
        if v in word_counter:
            data[i] = float(word_counter[v]) / norm
    return SparseVector(vocablen, data)


def process_docs_from_s3(filepath):
    """Downloads, unzips and chop the file into individual documents

    Input: relative paath of common crawl gz file

    Output: yields documents and uri for spark job to collect

    Examples:
        >>> print(tokenize_text("today is a good day."))
         (http://domain.com/topic.html,
         WARC/1.0
         WARC-Type: conversion
         WARC-Date: 2015-07-02T17:46:11Z
         WARC-Record-ID: <urn:uuid:cbb4ee19-dbb0-42f4-bb57-3903b04b4410>
         WARC-Refers-To: <urn:uuid:75191803-8a85-45b9-9eb9-338148e7314c>
         WARC-Block-Digest: sha1:4GDAGGRBW247XCFOYV7A7XJWR3XSUXFF
         Content-Length: 3671

         Not A Pretty Girl by Ani Difranco - YouTube Skip navigation
         UploadSign inSearch
         Loading...
         Close
         Yeah, keep it
         Undo
         Close
         This video is unavailable.
         Watch QueueTV QueueWatch QueueTV Queue Remove allDisconnect
         Loading...
         Watch Queue
         TV Queue
         __count__/__total__
         Find out whyClose
         Not A Pretty Girl by Ani Difranco
         dejasoul7's channel
         SubscribeSubscribedUnsubscribe93 Subscription preferences
         Loading...
         Loading...
         Working...)
    """
    k = Key(pds, filepath)
    temp = tempfile.NamedTemporaryFile(suffix='.gz')
    k.get_contents_to_filename(temp.name)
    uri = None
    doc = None
    plaintext = False
    with gzip.open(temp.name, 'r') as fin:
        for line in fin:
            if line[0:13] == 'Content-Type:':
                plaintext = line.split('Content-Type:')[1].lower().strip() == 'text/plain'
                doc = ' '.join([doc, line])
            elif line[0:8] == "WARC/1.0":
                if doc is not None and plaintext:
                    yield doc
                doc = line
                plaintext = False
            else:
                doc = ' '.join([doc, line])


def get_cos_sim(docziptfidf, clusters):
    """Gets cosine similarity between a document and closest cluster
    Input: zipped docrdd and tfidf row, kmeans cluster object

    Output: cluster number and a tuple representing cosine similarity along with original document
    """
    vec = docziptfidf[1].toArray()
    cluster_num = clusters.predict(vec)
    clusteraccum[cluster_num].add(1)
    cossim =  (1 - spatial.distance.cosine(vec, clusters.clusterCenters[cluster_num]))
    return cluster_num, (cossim, docziptfidf[0])


def doc_with_keyword(doc,keywords):
    """returns a boolean after checking whether the doc has one of the keywords
    Input: document, list of keywords

    Output: True or False
    """
    count = 0;
    for token in tokenize_text(doc):
        if token in keywords:
            count +=1
            if count > 1 : return True
    return False


if __name__ == "__main__":
  print "hello"
  config = ConfigParser.ConfigParser()
  config.read('websclae_nlp.config')
  parallelism = int(config.get('SPARK', 'parallelism'))
  master_type = config.get('AWS', 'master_type')
  slave_type = config.get('AWS', 'slave_type')
  num_slaves = config.get('AWS', 'num_slaves')
  con_str = "dbname=" + config.get('POSTGRES', 'dbname')
  con_str += " user=" + config.get('POSTGRES', 'user')
  con_str += " host=" + config.get('POSTGRES', 'host')
  con_str += " password=" + config.get('POSTGRES', 'password')
  num_files_to_process = int(config.get('COMMON_CRAWL', 'num_files_to_process'))
  year_month = config.get('COMMON_CRAWL', 'year_month')
  filtered_docs_only = bool(config.get('COMMON_CRAWL', 'filtered_docs_only'))


  # In[7]:

  print
  print
  print "Lanuching process with following parameters:"
  print "parallelism : ", parallelism
  print "master_type : ", master_type
  print "slave_type : ", slave_type
  print "num_slaves : ", num_slaves
  print "DB Conn Str : ", con_str
  print "Year-Month to process : ", year_month
  print "Number of files to process : ", num_files_to_process
  print "Filter documents on keywords : ", filtered_docs_only
  print "press ctrl + c in next 10 seconds to abort"
  print "will continue in 10 seconds"
  print
  print
  time.sleep(10)


  print "starting"
  sys.stdout.flush()


  if parallelism > 1:
      conf = SparkConf().set("spark.default.parallelism", parallelism)
  else:
      conf = SparkConf()

  sc = SparkContext(conf=conf)
  print "got spark context"
  sys.stdout.flush()



  stopwordsCache = set(stopwords.words('english'))
  regex = re.compile("[^-.a-zA-Z]")


  with open('exclude_list.txt') as f:
      exclude_list = tokenize_text(f.read())

  #use standard vocabulary
  with open('vocab.txt') as f:
      vocabulary = tokenize_text(f.read())
  vocabulary = vocabulary[0:25000]

  if filtered_docs_only:
      with open('filter_words.txt') as f:
          filter_words = tokenize_text(f.read())
      #include filter words and phrases
      for item in filter_words:
          vocabulary.append(item)

  #exclude internet noise words
  for item in vocabulary:
      if len(item)<3:
          vocabulary.remove(item)

  for item in vocabulary:
      if item in exclude_list:
          vocabulary.remove(item)

  vocabulary = map(str.lower, vocabulary)
  vocabulary = sorted(list(set(vocabulary)))

  vocablen=len(vocabulary)




  #language lookup
  lang_lookup = {'gv': 'Manx', 'gu': 'Gujarati', 'gd': 'Gaelic; Scottish Gaelic', 'ga': 'Irish',
                 'gn': 'Guarani', 'gl': 'Galician', 'lg': 'Ganda', 'lb': 'Luxembourgish; Letzeburgesch',
                 'la': 'Latin', 'ln': 'Lingala', 'lo': 'Lao', 'tt': 'Tatar', 'tr': 'Turkish', 'ts': 'Tsonga',
                 'li': 'Limburgan; Limburger; Limburgish', 'lv': 'Latvian', 'to': 'Tonga (Tonga Islands)',
                 'lt': 'Lithuanian', 'lu': 'Luba-Katanga', 'Mi': 'Micmac', 'tk': 'Turkmen', 'th': 'Thai',
                 'ti': 'Tigrinya', 'tg': 'Tajik', 'te': 'Telugu', 'ta': 'Tamil', 'yi': 'Yiddish',
                 'yo': 'Yoruba', 'de': 'German', 'da': 'Danish', 'dz': 'Dzongkha', 'st': 'Sotho, Southern',
                 'dv': 'Divehi; Dhivehi; Maldivian', 'qu': 'Quechua', 'Ga': 'Georgian',
                 'el': 'Greek, Modern (1453-)', 'eo': 'Esperanto', 'en': 'English', 'zh': 'Chinese',
                 'ee': 'Ewe', 'za': 'Zhuang; Chuang', 'mh': 'Marshallese', 'uk': 'Ukrainian',
                 'eu': 'Basque', 'et': 'Estonian', 'es': 'Spanish', 'ru': 'Russian',
                 'rw': 'Kinyarwanda', 'rm': 'Romansh', 'rn': 'Rundi', 'ro': 'Romanian; Moldavian; Moldovan',
                 'bn': 'Bengali', 'be': 'Belarusian', 'bg': 'Bulgarian', 'ba': 'Bashkir', 'wa': 'Walloon',
                 'wo': 'Wolof', 'bm': 'Bambara', 'jv': 'Javanese', 'bo': 'Tibetan', 'bh': 'Bihari languages',
                 'bi': 'Bislama', 'br': 'Breton', 'bs': 'Bosnian', 'ja': 'Japanese', 'om': 'Oromo',
                 'oj': 'Ojibwa', 'ty': 'Tahitian', 'oc': 'Occitan (post 1500)', 'tw': 'Twi',
                 'os': 'Ossetian; Ossetic', 'or': 'Oriya', 'xh': 'Xhosa', 'ch': 'Chamorro',
                 'co': 'Corsican', 'ca': 'Catalan; Valencian', 'ce': 'Chechen', 'cy': 'Welsh',
                 'cs': 'Czech', 'cr': 'Cree', 'cv': 'Chuvash',
                 'cu': 'Church Slavic; Old Slavonic; Church Slavonic; Old Bulgarian; Old Church Slavonic',
                 've': 'Venda', 'ps': 'Pushto; Pashto', 'pt': 'Portuguese', 'tl': 'Tagalog',
                 'pa': 'Panjabi; Punjabi', 'vi': 'Vietnamese', 'pi': 'Pali', 'is': 'Icelandic',
                 'pl': 'Polish', 'hz': 'Herero', 'hy': 'Armenian', 'hr': 'Croatian', 'iu': 'Inuktitut',
                 'ht': 'Haitian; Haitian Creole', 'hu': 'Hungarian', 'hi': 'Hindi', 'ho': 'Hiri Motu',
                 'ha': 'Hausa', 'he': 'Hebrew', 'mg': 'Malagasy', 'uz': 'Uzbek', 'ml': 'Malayalam',
                 'mn': 'Mongolian', 'mi': 'Maori', 'ik': 'Inupiaq', 'mk': 'Macedonian', 'ur': 'Urdu',
                 'mt': 'Maltese', 'ms': 'Malay', 'mr': 'Marathi', 'ug': 'Uighur; Uyghur', 'my': 'Burmese',
                 'ki': 'Kikuyu; Gikuyu', 'aa': 'Afar', 'ab': 'Abkhazian', 'ae': 'Avestan', 'ss': 'Swati',
                 'af': 'Afrikaans', 'tn': 'Tswana', 'sw': 'Swahili', 'ak': 'Akan', 'am': 'Amharic',
                 'it': 'Italian', 'an': 'Aragonese', 'ii': 'Sichuan Yi; Nuosu',
                 'ia': 'Interlingua (International Auxiliary Language Association)', 'as': 'Assamese',
                 'ar': 'Arabic', 'su': 'Sundanese', 'io': 'Ido', 'av': 'Avaric', 'ay': 'Aymara',
                 'az': 'Azerbaijani', 'ie': 'Interlingue; Occidental', 'id': 'Indonesian', 'ig': 'Igbo',
                 'sk': 'Slovak', 'sr': 'Serbian', 'nl': 'Dutch; Flemish',
                 'nn': 'Norwegian Nynorsk; Nynorsk, Norwegian', 'no': 'Norwegian', 'na': 'Nauru',
                 'nb': 'Bokm\xc3\xa5l, Norwegian; Norwegian Bokm\xc3\xa5l',
                 'nd': 'Ndebele, North; North Ndebele', 'ne': 'Nepali', 'ng': 'Ndonga',
                 'vo': 'Volap\xc3\xbck', 'zu': 'Zulu', 'so': 'Somali', 'nr': 'Ndebele, South; South Ndebele',
                 'nv': 'Navajo; Navaho', 'sn': 'Shona', 'fr': 'French', 'sm': 'Samoan', 'fy': 'Western Frisian',
                 'sv': 'Swedish', 'fa': 'Persian', 'ff': 'Fulah', 'fi': 'Finnish', 'fj': 'Fijian',
                 'sa': 'Sanskrit', 'fo': 'Faroese', 'ka': 'Georgian', 'kg': 'Kongo', 'kk': 'Kazakh',
                 'kj': 'Kuanyama; Kwanyama', 'sq': 'Albanian', 'ko': 'Korean', 'kn': 'Kannada',
                 'km': 'Central Khmer', 'kl': 'Kalaallisut; Greenlandic', 'ks': 'Kashmiri', 'kr': 'Kanuri',
                 'si': 'Sinhala; Sinhalese', 'kw': 'Cornish', 'kv': 'Komi', 'ku': 'Kurdish',
                 'sl': 'Slovenian', 'sc': 'Sardinian', 'ky': 'Kirghiz; Kyrgyz', 'sg': 'Sango',
                 'se': 'Northern Sami', 'sd': 'Sindhi'}





  #get file paths
  cd = CrawlData(con_str)
  small_list = cd.get_random_paths(year_month,num_files_to_process)
  print "processing %s files" % len(small_list)
  sys.stdout.flush()

  conn = boto.connect_s3()
  pds = conn.get_bucket('aws-publicdatasets')
  print "got connection to aws s3 bucket"
  sys.stdout.flush()

  #create language dict to store language accumulators in order
  langdict = {}
  for i, item in enumerate(lang_lookup):
      langdict[item] = i

  #store spark accumulators in array for language identifier to use it to store language counts
  #this is more efficient as it stores language counts while we are passing throught
  langaccum = []
  for k in langdict:
      langaccum.append(sc.accumulator(0))

  jobid = cd.create_job(master_type=master_type,slave_type=slave_type,num_slaves=num_slaves,jobtype=1)




  #create rdd for tfidf

  docsrdd = sc.parallelize(small_list).flatMap(lambda x: process_docs_from_s3(x))   .map(lambda x : (detect_lang(x),x)).filter(lambda x : x[0] == 'en').map(lambda x: x[1])
  if filtered_docs_only:
     docsrdd = docsrdd.filter(lambda x : doc_with_keyword(x,filter_words))


  #docsrdd = docsrdd.sample(False, 0.01)
  docsrdd.cache()
  tf = docsrdd.map(lambda x : get_word_vec(x, vocabulary, vocablen))
  tf.cache()
  numdocs = tf.count()
  print "Number of Docs: ", numdocs
  print "tf created and cached"
  sys.stdout.flush()
  idf = IDF().fit(tf)
  print "idf created"
  sys.stdout.flush()
  tfidf = idf.transform(tf)
  tfidf.cache()
  print "tfidf created"




  #run k-means
  numclusters = min(int(np.sqrt(numdocs/2)),100)
  print "looking for %s clusters" %(numclusters)
  sys.stdout.flush()
  clusters = KMeans.train(tfidf, numclusters)
  print "found clusters", numclusters
  sys.stdout.flush()

  #initilize spark accumulators for finding out number of docs per cluster
  #store kew words and weights in database
  clusteraccum = []
  for i in range(numclusters):
      clusteraccum.append(sc.accumulator(0))
      words = np.array(vocabulary)[np.argsort(-clusters.clusterCenters[i])[0:100]]
      weights = np.array(sorted(clusters.clusterCenters[i],reverse=True)[0:100])
      cd.store_cluster_kw(jobid, i, words.tolist(),weights.tolist())




  #find top document for each cluster
  topdocs = docsrdd.zip(tfidf).map(lambda x : get_cos_sim(x, clusters)).reduceByKey(lambda a, b : a if a[0] > b[0] else b).collect()
  #store number of docments per cluster
  d = {}
  for i, accum in enumerate(clusteraccum):
      d[i] = accum.value
  cd.store_cluster_info(jobid,d)

  #store top documents for each cluster
  d = sorted(d.items(), key=operator.itemgetter(1),reverse=True)
  td={}
  for i,(cid, numdocs) in enumerate(d):
      if i >= 100:
          break;
      for doc in topdocs:
          if doc[0] == cid:
              td[cid] = [(doc[1][0],doc[1][1])]
              break
  cd.store_cluster_topdocs(jobid,td)
  #update file paths with job_id
  cd.update_job_id_for_file_paths(year_month,small_list,jobid)



  d = {}
  val = 0
  for k,v in langdict.iteritems():
      val = langaccum[v].value
      if val > 0 :
          d[k] = langaccum[v].value
  cd.store_lang_info(jobid,d)
  cd.close_job(jobid)





