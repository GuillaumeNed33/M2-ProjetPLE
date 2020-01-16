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
    
    $ spark-submit \
        --master yarn \
        --deploy-mode cluster \
        --num-executors 15 \
        --total-executor-cores 8 \
        --executor-memory 4G \
        --class bigdata.MainPLE target/ProjetPLE-0.0.1.jar \
        <questionID> <otherArguments>
    
Les différentes possibilités de question sont les suivantes : 
| **1a** | **1b** | **1c** | **2** | **3** | **4** | **5** | **6** | **7** |

Pour la question 7, il faut renseigner exactement 4 patterns en arguments pour que le programme s'execute.  
Les patterns ciblés par le programmes doivent être donnés en arguments avec leur identifiants. Par exemple, si on souhaite utiliser les patterns "1", "2", "3" et "4", la commande serait : 

    $ spark-submit \
        --master yarn \
        --deploy-mode cluster \
        --num-executors 15 \
        --total-executor-cores 8 \
        --executor-memory 4G \
        --class bigdata.MainPLE target/ProjetPLE-0.0.1.jar \
        7 1 2 3 4

Les chemins vers les fichiers `phases.csv`, `jobs.csv` et `patterns.csv` sont écrits en durs dans le code.  

## Fichiers de sorties

Pour chaque question, le chemin de sortie est `/user/gnedelec001/output/`.
Vous pouvez consultez les résultats avec la commande suivante :

    $ hdfs dfs -cat <path_to_file>
    
Pour chacune des questions voici les chemins des résultats :

* 1a : `/user/gnedelec001/output/1a_DurationDistribution_NotIDLE/part-00000`
* 1b : `/user/gnedelec001/output/1b_DurationDistribution_IDLE/part-00000`
* 1c : `/user/gnedelec001/output/1c_DurationDistribution_AlonePattern/part-00000`
* 2  : `/user/gnedelec001/output/2_NbPatternsDistributionPerPhase/part-00000`
* 3  : `/user/gnedelec001/output/3_NbJobsDistributionPerPhase/part-00000`
* 4a  : `/user/gnedelec001/output/4a_DistributionJobs/part-00000`
* 4b  : `/user/gnedelec001/output/4b_DistributionJobs/part-00000`
* 5  : `/user/gnedelec001/output/5_getTotalDurationIDLE/part-00000`
* 6a  : `/user/gnedelec001/output/6a_PercentOfTotalTimeForPattern/part-00000`
* 6b  : `/user/gnedelec001/output/6b_top10PercentOfTotalTime/part-00000`
* 7  : `/user/gnedelec001/output/7_getLinesMatchingWithPatterns_1_2_3_4/part-00000`

Pour le chemin de la question 7, remplacez les `1_2_3_4` par les patterns mis en paramètre de l'execution.