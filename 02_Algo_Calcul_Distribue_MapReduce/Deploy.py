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


####################################################################################
        
def create_arborescence(machine):
#    for m in machines:
    cmd = 'ssh jmaksoud@' + machine + ' mkdir -p /tmp/jmaksoud/shufflesreceived /tmp/jmaksoud/map /tmp/jmaksoud/reduce /tmp/jmaksoud/split /tmp/jmaksoud/shuffles'
    monProcess = sp.run(cmd, shell=True, capture_output=True, timeout=10)

    
###############################################################################

            
#Fonction qui copie un fichier sur une machine
def copie_du_slave(machine):
#    for m in machines:      
        try:            
            cmd = 'scp /cal/homes/jmaksoud/INF727/Slave.py jmaksoud@' + machine + ':/tmp/jmaksoud/'
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
                print('Copie du slave.py sur machine ' , machine , ' : So far so good')
            pass


###############################################################################

if __name__ == '__main__':
    #Recuperation des noms de machines

    
    create_arborescence(Mes3Ip)
    
    #Copie du fichier Slave.py sur toutes les machines allumees (en parallele)
    
   