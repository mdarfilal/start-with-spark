%pyspark
# Q1 - Chargez une liste d'entiers de 1 � 8 dans un RDD.
k = sc.parallelize([1,2,3,4,5,6,7,8])
----------------------------------------------------------------------------
%pyspark
# On va importer la librairie random pour faire notre test
import random

# Q2 - Affichez les dix premiers entiers de votre liste k
print (k.take(10))

# Q3 - Cr�ez un RDD nomm� rdd, bas� sur k, qui pour chaque �l�ment de k
# lui ajoute un nombre al�atoire entre 0 et 5
# Astuce : pour un nombre al�atoire entre 1 et 10 : random.randint(1,10) 
rdd = k.map(lambda x: x + random.randint(0,5))

Result :
[1, 2, 3, 4, 5, 6, 8, 8]
----------------------------------------------------------------------------
%pyspark

# Q4 - Affichez une premi�re fois le contenu du RDD "rdd"
print(rdd.collect())

# Q5 - Affichez une seconde fois le contenu du RDD "rdd", que remarquez-vous ?
print(rdd.collect())

# On remarque que les valeurs ne sont pas affich�es dans le m�me ordre

Result :
[4, 3, 7, 5, 8, 7, 12, 9]
[3, 7, 4, 5, 7, 11, 9, 9]
----------------------------------------------------------------------------
%pyspark


# Q6 - Mettez le RDD "rdd" en cache
rdd.cache()

# Q7 - Affichez une premi�re fois le contenu du rdd "rdd"
print(rdd.collect())

# Q8 - Affichez une seconde fois le contenu du rdd "rdd", que remarquez-vous ?
print(rdd.collect())

# On remarque que les valeurs sont affich�es dans le m�me ordre

Result :
[4, 3, 7, 5, 8, 7, 12, 9]
[4, 3, 7, 5, 8, 7, 12, 9]
----------------------------------------------------------------------------
%pyspark
# Q9 - Cr�ez un rdd k2 � partir d'une liste de 1 � 8 sur 3 partitions
k2 = sc.parallelize([1,2,3,4,5,6,7,8], 3)

# Q9 -bis Cr�ez un rdd k3 � partir d'une liste de 1 � 8 sur 6 partitions
k3 = sc.parallelize([1,2,3,4,5,6,7,8], 6)

# Q10 - Affichez le contenu des deux RDD
print(k2.collect())
print(k3.collect())

Result :
[1, 2, 3, 4, 5, 6, 7, 8]
[1, 2, 3, 4, 5, 6, 7, 8]
----------------------------------------------------------------------------
%pyspark

# Q11 - Executez un reduce, en divisant chaque �l�ment de la liste par l'�l�ment suivant sur k2
res2 = k2.reduce(lambda x,y: x/y)

# Q11 bis - M�me chose sur k3
res3 = k3.reduce(lambda x,y: x/y)

# Q12 - Affichez le r�sultat res2 et res3
print(res2)
print(res3)

Result :
44.8
0.025396825396825397
----------------------------------------------------------------------------
%pyspark
# Q13 - Il est possible de r�duire le nombre de partition avec la m�thode 
# coalesce(n) avec n le nombre de partitions
# Compl�tez le reduce avec la division d'un l'�l�ment de la liste par son successeur
# (identique Q11)
print(k2.coalesce(1).reduce(lambda x,y: x/y))
print(k3.coalesce(1).reduce(lambda x,y: x/y))

Result :
2.48015873015873e-05
2.48015873015873e-05