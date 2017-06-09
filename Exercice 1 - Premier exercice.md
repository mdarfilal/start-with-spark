%pyspark
# On charge une liste d entiers de 1 à 10
# Q1 - Complétez le code suivant en renseignant la liste d'entiers à charger
maListe = sc.parallelize([1,2,3,4,5,6,7,8,9,10])

# Q2 - Affichez le contenu de la liste
print(maListe.take(10))

Result :
[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
----------------------------------------------------------------------------
%pyspark
# Q3 - Multiplier chacune des valeurs de la collection par 10
maListe = maListe.map(lambda v: v * 10)

# Affichez si vous le souhaitez les valeurs de la nouvelle collecte
print(maListe.take(10))

# Q4 - Filtrer les valeurs de la liste pour ne conserver que les valeurs inférieures à 100
maListe = maListe.filter(lambda v: v < 100)

# Affichez les valeurs de la liste pour vérifier le résultat
print(maListe.collect())

Result :
[10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
[10, 20, 30, 40, 50, 60, 70, 80, 90]
----------------------------------------------------------------------------
%pyspark
# Q5 - Créez la fonction sup50 qui prend en argument x
# et qui retourne la chaine "sup50" si x est > 50
#                 la chaine "inf50" sinon
def sup50(x) :
    return "sup50" if x > 50 else "inf50"
----------------------------------------------------------------------------
%pyspark
# Q6 - Appliquez la fonction précedemment créée
# Afin de transformer votre liste d'entiers n en liste de tuples
# (categorie,entier) avec categorie = sup50 et ou inf50 selon l'entier
maListe = maListe.map(lambda x: (sup50(x), x))

# Affichez les valeurs de votre liste pour vérifier le résultat
print(maListe.collect())

Result :
[('inf50', 10), ('inf50', 20), ('inf50', 30), ('inf50', 40), ('inf50', 50), ('sup50', 60), ('sup50', 70), ('sup50', 80), ('sup50', 90)]
----------------------------------------------------------------------------
%pyspark
# Q7 - Utilisez la fonction reduceByKey pour sommer les valeurs de chaque catégorie
parCategorie = maListe.reduceByKey(lambda x,y: x+y)

# Affichez le résultat
print(parCategorie.collect())

Result :
[('inf50', 150), ('sup50', 300)]