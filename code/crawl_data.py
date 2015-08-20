from preparing_cursor import PreparingCursor
import psycopg2
import time
import random


class CrawlData(object):
    """Database helper for logging common crawl data into Database"""
    def __init__(self, connstr="dbname='common_crawl' user='Farhan' host='localhost' password=''"):
        self.connstr = connstr

    def get_cursor(self):
        """helper function to get a cursor object to postgres database

        Input: None

        Output: Postgres DB cursor
        """
        conn = psycopg2.connect(self.connstr)
        conn.autocommit = True
        cur = conn.cursor(cursor_factory=PreparingCursor)
        return cur

    def create_job(self,master_type='r3.xlarge',slave_type='r3.xlarge',num_slaves=19, jobtype=1):
        """helper function to store job details in postgres database

        Input: type of master node, type of slave node, number of slaves and a job type ['clustering' = 0, 'lang_only' = 1]

        Output: JobID
        """
        cur = self.get_cursor()
        ps = cur.prepare("INSERT INTO job (start_ts, master_type, slave_type, num_slaves,job_status,job_type) Values(%s, %s, %s, %s, %s, %s)")
        cur.execute((time.time(),master_type, slave_type,num_slaves,'STARTED',jobtype))
        cur.prepare("SELECT currval(pg_get_serial_sequence('job', 'job_id'))")
        cur.execute()
        return cur.fetchall()[0][0]

    def close_job(self,job_id,job_status='COMPLETED'):
        """helper function to close job and update associated database timestamp

        Input: ID of job to be closed, job status flag ['STARTED', 'COMPLETED', 'ERROR']

        Output: None
        """
        cur = self.get_cursor()
        ps = cur.prepare("UPDATE job SET end_ts = %s, job_status=%s WHERE job_id=%s")
        cur.execute((time.time(),job_status, job_id))

    def store_lang_info(self,job_id,langcountdict):
        """helper function to store language document counts for a job

        Input: ID of job to be closed, dict object representing language counts e.g. {'en' : 551, 'fr':23}
        Output: None
        """
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
                       'pa': 'Punjabi', 'vi': 'Vietnamese', 'pi': 'Pali', 'is': 'Icelandic',
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


        cur = self.get_cursor()
        ps = cur.prepare("INSERT INTO job_languages (job_id, lang, count) Values(%s, %s, %s)")
        for k, v in langcountdict.iteritems():
            if k in lang_lookup:
                lang = lang_lookup[k]
                cur.execute((job_id,lang, v))

    def store_cluster_info(self,job_id,clusterdoccountdict):
        """helper function to store language metadata for a job

        Input: ID of job, dict representing number of documents per cluster e.g. {0:1557, 1:343, 2:443}
        Output: None
        """
        cur = self.get_cursor()
        ps = cur.prepare("INSERT INTO job_clusters (job_id, cluster_id, num_docs) Values(%s, %s, %s)")
        for k, v in clusterdoccountdict.iteritems():
            cur.execute((job_id,k, v))

    def store_cluster_kw(self,job_id,cluster_id,keywords,keyword_weights):
        """helper function to store keywords and keyword weights for a cluster
        Input: ID of job, id number of the cluster, list object representing keywords,  list object representing wieght of each keyword
        Output: None
        """
        cur = self.get_cursor()
        ps = cur.prepare("INSERT INTO cluster_keywords (job_id, cluster_id, keywords, keyword_weights) Values(%s, %s, %s, %s)")
        cur.execute((job_id,cluster_id, keywords, keyword_weights))

    def store_cluster_topdocs(self,job_id,clustertopdocsdict):
        """helper function to store top documents by cluster for a job
        Input: ID of job, dict representing top documents for each cluster e.g {0:['text of doc1', 'text of doc2']}
        Output: None
        """
        cur = self.get_cursor()
        ps = cur.prepare("INSERT INTO cluster_topdocs (job_id, cluster_id, doc_order_id, top_doc) Values(%s, %s, %s, %s)")
        for k, v in clustertopdocsdict.iteritems():
            for i, top_doc in enumerate(v):
                cur.execute((job_id,k, i, top_doc))

    def job_log_error(self,job_id,err_text):
        """helper function to store error text associated with a job
        Input: ID of job, error string
        Output: None
        """
        self.closejob(job_id,job_status='ERROR')
        cur = self.get_cursor()
        ps = cur.prepare("INSERT INTO job_errors (job_id, err_text) Values(%s, %s)")
        cur.execute((job_id, err_text))


    def store_file_paths(self,yearmonth, filepathlist):
        """helper function to store common crawl file paths in postgres database
        Input: year and mon of the file e.g. '2015-06', relative path to aws public datatsets bucket on S3 for the file
        Output: None
        """
        cur = self.get_cursor()
        ps = cur.prepare("INSERT INTO file_paths(year_month, file_path) Values(%s,%s)")
        for item in filepathlist:
            cur.execute((yearmonth, item))


    def update_job_id_for_file_paths(self,yearmonth,filepathlist, job_id):
        """helper function to associate job id of the job that processed a common crawl file with the file metadata
        Input: year and mon of the file e.g. '2015-06', list of file path to be updated, job id of the job that processed the docs
        Output: None
        """
        cur = self.get_cursor()
        ps = cur.prepare("UPDATE file_paths SET job_id = %s WHERE file_path=%s and year_month=%s and job_id IS NULL")
        for item in filepathlist:
            cur.execute((job_id, item,yearmonth))

    def get_random_paths(self,yearmonth, numfiles):
        """helper function to get randomized file paths to common crawl files to process
        Input: year and mon of the file e.g. '2015-06', number of files
        Output: None
        """
        cur = self.get_cursor()
        ps = cur.prepare("SELECT  file_path from  file_paths WHERE year_month=%s and file_path != %s and job_id IS NULL")
        cur.execute((yearmonth,''))
        rows = cur.fetchall()
        lstpaths = []
        for row in rows:
            lstpaths.append(row[0])
        if numfiles > 0 :
            return random.sample(lstpaths,numfiles)
        else:
            return lstpaths


    def get_cluster_keywords(self,job_id,numclusters=20):
        """helper function to get saved cluster keywords for a given job
        Input: ID of job
        Output: cluster id, cluster label, list of keywords, list of keyword weights
        """
        cur = self.get_cursor()
        ps = cur.prepare("SELECT  cluster_id, keywords,keyword_weights from  cluster_keywords WHERE job_id=%s and job_id !=%s")
        cur.execute((job_id,0))
        kwdata = cur.fetchall()


        cur = self.get_cursor()
        ps = cur.prepare("SELECT  cluster_id, num_docs, cluster_label from  job_clusters WHERE job_id=%s and job_id !=%s ORDER BY num_docs DESC")
        cur.execute((job_id,0))
        jobclusters = cur.fetchall()

        #numdocs[i][1],
        d = []
        for i , (cid,numdocs,label) in enumerate(jobclusters):
            if i > numclusters : break
            d.append((cid,label,numdocs,kwdata[cid][1].replace('{','').replace('}','').split(','),kwdata[cid][2].replace('{','').replace('}','').split(',')))

        return d

    def get_cluster_topdocs(self,job_id,numclusters=20):
        """helper function to get saved cluster top documents
        Input: ID of job
        Output: cluster id, doc order id, document
        """
        cur = self.get_cursor()
        ps = cur.prepare("SELECT  cluster_id, doc_order_id,top_doc from  cluster_topdocs WHERE job_id=%s and job_id !=%s")
        cur.execute((job_id,0))
        topdocs = cur.fetchall()

        cur = self.get_cursor()
        ps = cur.prepare("SELECT  cluster_id, num_docs from  job_clusters WHERE job_id=%s and job_id !=%s ORDER BY num_docs DESC")
        cur.execute((job_id,0))
        jobclusters = cur.fetchall()

        d = []
        for i , (cid,numdocs) in enumerate(jobclusters):
            if i > numclusters : break
            for j , (tcid,order_id, doc) in enumerate(topdocs):
                if cid == tcid:
                    d.append((cid,order_id,doc))
                    break;
        return d
