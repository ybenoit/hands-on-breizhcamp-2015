/**** Scala 101 ****/
// Definir une variable
val myVariable = 2

// Definir une fonction
def sum(n1: Double, n2: Double) = {
  n1 + n2
}

// Définir et lire un tuple
val myTuple = ("key", 1)
println(myTuple._1) // Affiche key
println(myTuple._2) // Affiche 1

// Définir et lire un Array
val myArray = Array(1,2,3,4)
println(myArray(0)) // Retourne 1
println(myArray(2)) // Retourne 3


/**** Functionnal Programming 101 ****/
// Map
myArray.map(x => x + 1) // Retourne Array(2,3,4,5)
// Filter
myArray.filter(x => x > 3) // Retourne Array(4,5)


/**** Spark 101 ****/
// Lire un fichier
val myData = sc.textFile("README.md")

// Compter les lignes
myData.count()

// Récupérer la première ligne
myData.first()

// Prendre les 10 premières ligne d'un RDD
val first10 = myData.take(10)

// Les afficher
first10.foreach(println)

// Filtrer les lignes
val linesWithSpark = myData.filter(line => line.contains("Spark"))
linesWithSpark.count()

// Monter les données en mémoire
myData.cache()


/**** MLlib 101 ****/
// Entrainer un modèle
val myTrainData: RDD[LabeledPoint] = ...  // ou RDD[Vector]
val model = MyModel.train(myTrainData, params) // Retourne un modèle entrainé

// Faire des prédictions
val myTestData: RDD[LabeledPoint] = ... 
val predictions: RDD[Int] = myTestData.map(l => model.predict(l.features)) // Retourne un RDD contenant les prédictions


/**************
Pour aller plus loin : 
https://github.com/xbucchiotty/scala-for-beginners
https://github.com/xbucchiotty/codeur_en_seine 
***************/