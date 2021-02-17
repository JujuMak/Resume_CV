import pandas as pd
import subprocess as sp
import multiprocessing as mp
import time
from functools import partial

#Fonction qui recupere les noms de machine dans le csv et qui renvoit une liste
def openFile(file):
    machinesCSV = pd.read_csv("Machines_TP.csv")
    return machinesCSV["Machines"].values.tolist()

#Fonction qui cherche si une machine de TP est allumee
def recherche(mach):
    #Construction de la commande systeme
    #cmd = 'ssh jmaksoud@' + mach + ' hostname'
    cmd = 'ssh ' + mach # + ' hostname'

    try:
        #execution de la commande systeme
        monProcess = sp.run(cmd, shell=True, capture_output=True, timeout=10)
    #Si le timeout est atteins
    except sp.TimeoutExpired:
        #print(mach + " eteinte")
        pass
    #Si la commande a renvoye un resultat
    else:
        #Si tout s'est bien passe
        if not monProcess.returncode:
            print('machine ' + mach + ' allumee' )
            return mach
        #Si on a reçu un message d'erreur
        else:
            #print("Probleme avec la machine " + mach)
            pass

#Fonction qui copie un fichier sur une machine
def nettoyerMachine(repertoire, machine):
    try:
        #tentative de creation du repertoire sur la machine distante
#        cmd = 'ssh jmaksoud@' + machine + ' rm -Rf ' + repertoire
        cmd = 'ssh ' + machine + ' rm -Rf ' + repertoire
        monProcess = sp.run(cmd, shell=True, capture_output=True, timeout=10)
        print(repertoire, ' supprime sur machine ' , machine)
    # Si le timeout est atteins
    except sp.TimeoutExpired:
        print(machine + " eteinte")
        pass
    # Si la commande a renvoye un resultat
    else:
        #Si on a reçu un message d'erreur
        if monProcess.returncode:
            print("Probleme de suppression avec la machine " + machine)
        else:
            print('.', end='')
        pass


if __name__ == '__main__':
    
#    #Recuperation des noms de machines
#    machines = openFile("Machines_TP.csv")
#
#    #Verification de l'etat des machines (en parallele)
#    machinesTemp = []
#    start = time.time()
#    with mp.Pool(10) as p:
#        machinesTemp = p.map(recherche, machines)
#
#    #On ne garde que les machines allumees
#    machinesAllumees = []
#    for val in machinesTemp:
#        if val != None:
#            machinesAllumees.append(val)
#    print('Nombre de machines allumees : ' + str(len(machinesAllumees)))
#    print('Trouvees en ' + str(time.time() - start) + ' s', end='\n\n')
    
    Mes3Ip = ['tp-1a201-03','tp-1a201-09','tp-1a201-11']

# Effacement du repertoire /tmp/vpoquet sur toutes les machines allumees (en parallele))/)/
    print('Debut de la suppression des repertoires')
    repertoire = '/tmp/jmaksoud/'
    # on construit une fonction partiel car la fonction map ne prend que 2 arguments
    fonction = partial(nettoyerMachine, repertoire)
    with mp.Pool(10) as p:
#        p.map(fonction, machinesAllumees)
        p.map(fonction, Mes3Ip)
        
    cmd = 'rm -Rf $HOME/INF727/ReduceResult'
    sp.run(cmd,shell=True, capture_output=True, timeout=10)

    print('')
    stdout = print('Fin de la suppression des repertoires')