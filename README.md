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
    
Pour lancer le programme, executez la commande suivante avec en arguments, le numéro de la question du sujet :
    
    $ spark-submit --deploy-mode cluster --class bigdata.MainPLE target/ProjetPLE-0.0.1.jar <questionID> <otherArguments>
    
Les différentes possibilités de question sont les suivantes : 
| **1a** | **1b** | **1c** | **2a** | **3a** | **4a** | **4b** | **5** | **6a** | **6b** | **7** |

Pour la question 7, il faut renseigner exactement 4 patterns en arguments pour que le programme s'execute.  
Les patterns ciblés par le programmes doivent être donnés en arguments avec leur identifiants. Par exemple, si on souhaite utiliser les patterns "1", "2", "3" et "4", la commande serait : 

    $ spark-submit --deploy-mode cluster --class bigdata.MainPLE target/ProjetPLE-0.0.1.jar 7 1 2 3 4

Les chemins vers les fichiers `phases.csv`, `jobs.csv` et `patterns.csv` sont écrits en durs dans le code.  
