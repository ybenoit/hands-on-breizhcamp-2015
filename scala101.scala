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

// Définir un Array
val myArray = Array(1,2,3,4)


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

// Filtrer les lignes
val linesWithSpark = myData.filter(line => line.contains("Spark"))
linesWithSpark.count()

// Monter les données en mémoire
myData.cache()
