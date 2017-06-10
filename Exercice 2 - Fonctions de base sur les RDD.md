%pyspark
# Q1 - Remplissage du RDD avec un corpus d'articles extraits d'un site très intéressant...
# Si vous déposez vos fichiers dans à la racine de l'environnement, ils seront visibles depuis Zeppelin dans /data/

# Impossible de charger les fichiers
articles = sc.textFile("./README.md")

# Q2 - Comptage du nombre de ligne collectees dans les articles
articles.count()

Result :
29
----------------------------------------------------------------------------
%pyspark
# Q3 - Afficher les 10 premieres lignes du RDD
articles.take(10)

Result :
['# Apache Zeppelin', '', '**Documentation:** [User Guide](http://zeppelin.apache.org/docs/latest/index.html)<br/>', '**Mailing Lists:** [User and Dev mailing list](http://zeppelin.apache.org/community.html)<br/>', '**Continuous Integration:** [![Build Status](https://travis-ci.org/apache/zeppelin.svg?branch=master)](https://travis-ci.org/apache/zeppelin) <br/>', '**Contributing:** [Contribution Guide](https://zeppelin.apache.org/contribution/contributions.html)<br/>', '**Issue Tracker:** [Jira](https://issues.apache.org/jira/browse/ZEPPELIN)<br/>', '**License:** [Apache 2.0](https://github.com/apache/zeppelin/blob/master/LICENSE)', '', '']
----------------------------------------------------------------------------
%pyspark
# Q4 - Eclatez chaque ligne du RDD articles en mots
# Astuce : La fonction split() en Python devrait vous être utile...
mots = articles.flatMap(lambda x: x.split(" "))

# Affichez le nombre de mots
print(mots.count())

# Affichez des 50 premiers mots
print(mots.take(50))

Result :
123
['#', 'Apache', 'Zeppelin', '', '**Documentation:**', '[User', 'Guide](http://zeppelin.apache.org/docs/latest/index.html)<br/>', '**Mailing', 'Lists:**', '[User', 'and', 'Dev', 'mailing', 'list](http://zeppelin.apache.org/community.html)<br/>', '**Continuous', 'Integration:**', '[![Build', 'Status](https://travis-ci.org/apache/zeppelin.svg?branch=master)](https://travis-ci.org/apache/zeppelin)', '<br/>', '**Contributing:**', '[Contribution', 'Guide](https://zeppelin.apache.org/contribution/contributions.html)<br/>', '**Issue', 'Tracker:**', '[Jira](https://issues.apache.org/jira/browse/ZEPPELIN)<br/>', '**License:**', '[Apache', '2.0](https://github.com/apache/zeppelin/blob/master/LICENSE)', '', '', '**Zeppelin**,', 'a', 'web-based', 'notebook', 'that', 'enables', 'interactive', 'data', 'analytics.', 'You', 'can', 'make', 'beautiful', 'data-driven,', 'interactive', 'and', 'collaborative', 'documents', 'with', 'SQL,']
----------------------------------------------------------------------------
%pyspark
# Q5 - Comptez le nombre de fois qu'apparaît le mot "CLS"
print("Nombre d'apparition de Zeppelin :", mots.filter(lambda x: x == "Zeppelin").count())

Result :
Nombre d'apparition de Zeppelin : 3