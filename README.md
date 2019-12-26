# Projet PLE 2019 - 2020

## Sujet et rapport
Le sujet du projet est disponible ici : [sujetPLE_2019.pdf](./sujetPLE_2019.pdf)  
Le rapport du projet est disponible ici : [rapportPLE_2019.pdf](./rapportPLE_2019.pdf)

## L'équipe
* Guillaume NEDELEC
* Dorian ROPERT

## Installation & Exécution
Pour compiler exécutez la commande suivante : 

    $ mvn package
    
Pour lancer le programme, executez la commande suivante :
    
    $ spark-submit --class bigdata.MainPLE target/ProjetPLE-0.0.1.jar <patterns>
    
Les chemins vers les fichiers `phases.csv`, `jobs.csv` et `patterns.csv` sont écrits en durs dans le code.  
Il faut exactement 4 patterns en arguments pour que le programme s'execute.    
Les patterns ciblés par le programmes doivent être donnés en arguments avec leur identifiants. Par exemple, si on souhaite utiliser les patterns "1", "2", "3" et "4", la commande serait : 

    $ spark-submit --class bigdata.MainPLE target/ProjetPLE-0.0.1.jar 1 2 3 4
