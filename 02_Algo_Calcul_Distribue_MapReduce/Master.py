#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 27 15:16:09 2020

@author: jmaksoud
"""

import pandas as pd
import numpy as np
import subprocess as sp
import multiprocessing as mp
import time
from functools import partial
from itertools import starmap
from itertools import product
import os
from pathos.pools import ProcessPool as Pool
import timeit
import Slave
import Deploy
import sys
import glob,os
from os import listdir
from os.path import isfile, join
import pathlib 
###############################################################################

#Fonction qui recupere les noms de machine dans le csv et qui renvoit une liste
def openFile(file):
    machinesCSV = pd.read_csv(file)
    #return machinesCSV["Machines"].values.tolist()
    return machinesCSV["Machines"].values.tolist()

###############################################################################
    
#Fonction qui cherche si une machine de TP est allumee
def recherche(mach):
    #Construction de la commande systeme
    cmd = 'ssh jmaksoud@' + mach 
    try:
        #Execution de la commande systeme
        monProcess = sp.run(cmd, shell=True, capture_output=True, timeout=10)
    #Si le timeout est atteins
    except sp.TimeoutExpired:
        #print(mach + " eteinte")
        pass
    #Si la commande a renvoye un resultat
    else:
        #Si tout s'est bien passe
        if not monProcess.returncode:
            print(mach)
            return mach
        #Si on a recu un message d'erreur
        else:
            #print(mach + " a un probleme et ne sera pas utilisee ")
            pass


###############################################################################

def liste_machines_allumees(machines):
    machinesTemp = []
    pool = Pool(nodes=10)
    machinesTemp = pool.map(recherche, machines)
    
    #On ne garde que les machines allumees
    machinesAllumees = []
    for val in machinesTemp:
        if val != None:
            machinesAllumees.append(val)
 
    return machinesAllumees
    

##############################################################################
           
#Fonction qui copie un fichier sur une machine
def envoi_des_splits(fichier, machine):
    
    try:            
        cmd = 'scp ' + fichier + ' jmaksoud@' + machine + ':/tmp/jmaksoud/split'
        monProcess = sp.run(cmd, shell=True, capture_output=True, timeout=10)

    # Si le timeout est atteins
    except sp.TimeoutExpired:
        print(machine, " eteinte")
        pass
    # Si la commande a renvoye un resultat
    else:
        #Si on a recu un message d'erreur
        if monProcess.returncode:
            print("Probleme de copie avec la machine ", machine)
            print(monProcess.stderr)
        #Si tout s'est bien passe
        else:
            print('Copie du split' + fichier + ' sur ' +  machine + ' : OK')
        pass

#################################################################################
def execution_du_map_sur_slave(fichier,machine):
#    for i in range(len(fichier)):
#    cmd = f'ssh {machine}'.format(machine)
    cmd = f'ssh {machine} python3.7 /tmp/jmaksoud/Slave.py 0 {fichier}'.format(machine,fichier)
    print(cmd)
    print(f'Map de {fichier} sur {machine}'.format(fichier,machine))
    sp.run(cmd,shell=True, capture_output=True, timeout=10)


######################################################################################

def ecriture_nom_machines_dans_fichier(machines):
    with open('/cal/homes/jmaksoud/INF727/machines.txt', 'w') as f:
        for item in machines:
            f.write("%s\n" % item)
    f.close()
   
######################################################################################
    
def envoi_des_noms_de_machines(machines):
    cmd = 'scp /cal/homes/jmaksoud/INF727/machines.txt ' + f'jmaksoud@{machines}:/tmp/jmaksoud/'.format(machines)
    monProcess = sp.run(cmd, shell=True, capture_output=True, timeout=10)
#######################################################################################
    
def prepare_shuffle_et_envoi_vers_autres_machines(machine):
   
    cmdX = f'ssh {machine} ls /tmp/jmaksoud/map'.format(machine)
    UM = sp.run(cmdX,shell=True, capture_output=True, timeout=10).stdout
    UM = str(UM)[2:-3]

    cmd2 = f'ssh {machine} python3.7 /tmp/jmaksoud/Slave.py 1 /tmp/jmaksoud/map/{UM}'.format(machine,UM)
#    print(machine + ' ' + cmd2)
    sp.run(cmd2,shell=True, capture_output=True, timeout=10)
#######################################################################################
    
def launch_reduce(machine):

    cmd3 = f'ssh {machine} python3.7 /tmp/jmaksoud/Slave.py 2'
    print(machine + ' ' + cmd3)
    sp.run(cmd3,shell=True, capture_output=True, timeout=10)    
    
#######################################################################################

def recover_reduce_and_output_result(Mes3Ip):

    final_text = []
    
    for machine in Mes3Ip:
        cmd = f'ssh {machine}:/tmp/jmaksoud/reduce'.format(machine)
        sp.run(cmd,shell=True, capture_output=True, timeout=10)
        cmd2 = 'ls -p /tmp/jmaksoud/reduce '
        sp.run(cmd2,shell=True, capture_output=True, timeout=10).stdout
        cmd = f'scp {machine}:/tmp/jmaksoud/reduce/* /cal/homes/jmaksoud/INF727/ReduceResult/{machine}'.format(machine)  # accesses reduce
        print(cmd)
        sp.run(cmd,shell=True, capture_output=True, timeout=10)
        
        path = f'/cal/homes/jmaksoud/INF727/ReduceResult/{machine}'.format(machine)
        reduces = glob.glob(path+"/*")
        if len(reduces) > 0:
            text = pd.concat([pd.read_csv(f,header=None, sep = ' ') for f in reduces])
            print('Result for machine ' , machine , ' is the following\n: ', text)
            final_text.append(text)

    return final_text




    #
    #    path = f'/cal/homes/jmaksoud/INF727/ReduceResult/{machine}'.format(machine)
    #
    #    reduce_files = [f for f in listdir(path) if isfile(join(path, f))]
    #    print('reduce files read in ' , machine , ' ', path, ' are: ' , reduce_files)
    #
    #    for filename in reduce_files:
    #        print(filename)
    #        full_filename = f'/cal/homes/jmaksoud/INF727/ReduceResult/{machine}/{filename}'.format(machine,filename)
    #        tmp = pd.read_csv(full_filename,header=None,sep =' ')
    #        print('text inside file\n ' , tmp[0])
    #        print(tmp[1])
    #        text.append({'word':tmp.iloc[0,0],'count':tmp.iloc[0,1]},ignore_index=True)


# =============================================================================
 
    


# Master.py


if __name__=='__main__':
# 
#### LISTING DE TOUTES LES MACHINES
#    print('##########################################################################')
#    print('Lecture du fichier contenant la liste des machines TP de Telecom') 
#    machines = openFile("/cal/homes/jmaksoud/INF727/Machines_TP.csv")
#    print('Fichier de liste des machines lues')    
#
#### LISTING DES MACHINES DISPONIBLES UNIQUEMENT
#    print('##########################################################################')
#    start = time.time()
#    machinesAllumees = liste_machines_allumees(machines[0:8])
#    print('Machines disponibles identifiees')
#    end = timeit.timeit()
#    print('Temps necessaire a pinger 9 machines' , (end - start), 'seconds') 
#
#### DEFINITION DES SPLITS A ENVOYER ET DES MACHINES QUI SERONT UTILISEES
    
    fichiersacopier = list(['/cal/homes/jmaksoud/INF727/S0.txt',
                       '/cal/homes/jmaksoud/INF727/S1.txt',
                       '/cal/homes/jmaksoud/INF727/S2.txt'])
    
    fichiers_noms = list(['S0.txt','S1.txt','S2.txt'])
    
    
#    indices = list(np.random.uniform(low=0,high=len(machinesAllumees),size=len(fichiersacopier)).astype(int))
#    Mes3Ip=machinesAllumees[indices]
    
#    Mes3Ip = list(machinesAllumees[0:3])   
    Mes3Ip = ['tp-1a201-03','tp-1a201-09','tp-1a201-11']
    print('Machines selected')
 
    
     # CREATION DE L'ARBORESCENCE SUR LES MACHINES DISTANTES ET ENVOI DE Slave.py 
    print('##########################################################################')
    print('Creation des dossiers sur machines distantes et copie des Slave.py')
    startb = timeit.timeit()
    for machine in Mes3Ip:
        cmd2 = f'mkdir -p /cal/homes/jmaksoud/INF727/ReduceResult/{machine}'.format(machine)
        monProcess = sp.run(cmd2, shell=True, capture_output=True, timeout=10)
    with mp.Pool(10) as p:
        p.map(Deploy.create_arborescence,Mes3Ip)    
        print('Arborescence creee sur les machines distantes')
        p.map(Deploy.copie_du_slave,Mes3Ip)
        print('Slave.py copie sur les machines distantes')
    endb = timeit.timeit()           
    print('Temps necessaire pour creer arborescence et copier Slave.py: ' , 1000*(endb - startb), 'milliseconds')
   
    
    # ENVOI DES SPLITS VERS LES MACHINES CHOISIES  AVEC PATHOS
    print('##########################################################################')
    print('Envoi des splits aux machines distantes')
    starta = timeit.timeit()
    pool = Pool(nodes=10)
    ans = pool.map(envoi_des_splits,fichiersacopier,Mes3Ip)
    enda = timeit.timeit()
    print('Temps necessaires a envoi des splits vers machines distantes' , 1000*(enda - starta), 'milliseconds')

    # EXECUTION DU MAP SUR MACHINES DISTANTES AVEC PATHOS
    print('##########################################################################')
    print('Execution du map sur slaves')
    start4 = timeit.timeit()   
    pool = Pool(nodes=10)
    pool.map(execution_du_map_sur_slave,fichiers_noms,Mes3Ip)
    end4 = timeit.timeit()
    print('MAP FINISHED - this step took ', 1000*(end4 - start4), 'milliseconds')
    
    # ENVOI DE LA LISTE DES MACHINES AUX SLAVES
    print('##########################################################################')
    print('Envoi de la liste des machines aux slaves')
    start6 = timeit.timeit()
    ecriture_nom_machines_dans_fichier(Mes3Ip)
    pool = Pool(nodes=10)
    pool.map(envoi_des_noms_de_machines,Mes3Ip)
    end6 = timeit.timeit()
    print('Envoi de la liste des machines aux slaves - this step took ', 1000*(end6 - start6), 'milliseconds')
    
    # PREP ET EXECUTION DU SHUFFLE
    print('##########################################################################')
    print('Début de préparation du shuffle')
    start5 = timeit.timeit()
    pool = Pool(nodes=10)
    pool.map(prepare_shuffle_et_envoi_vers_autres_machines,Mes3Ip)
    end5 = timeit.timeit()
    print('SHUFFLE TERMINE - this step took ', 1000*(end5 - start5), 'milliseconds')
    
    # PREP ET EXECUTION DU SHUFFLE
    print('##########################################################################')
    print('Début du Reduce')
    start6 = timeit.timeit()
    pool = Pool(nodes=10)
    pool.map(launch_reduce,Mes3Ip)
    end6 = timeit.timeit()
    print('REDUCE FINISHED - this step took ', 1000*(end6 - start6), 'milliseconds')    
    
    print('##########################################################################')
    print('Copie des resultats vers HOME directory')
    start7 = timeit.timeit()    
    final_count = []
    pool = Pool(nodes=10)
    pool.map(recover_reduce_and_output_result,Mes3Ip)

    end6 = timeit.timeit()
    print('REDUCE FINISHED - this step took ', 1000*(end6 - start6), 'milliseconds')    
    
    