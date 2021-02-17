import pandas as pd
import numpy as np
import subprocess as sp
import time
import pickle
import sys
import zlib
import csv
import os 
import glob
import re
from zipfile import ZipFile


#######################################################################################
##### La fonction Map prend le fichier et fait le map
def mapping(file):
    filename = f'/tmp/jmaksoud/split/{file}'
    UMname=filename.replace('split/S','map/UM')
     
#    text =  open(filename,"r", encoding='utf-8').read().replace("\n"," ").split()
    
    
    tmp =  open(filename,"r", encoding='utf-8').read()
    tmp =  open(filename,"r").read()

    text=re.sub("[^a-zA-Z\d\s:]","",tmp).split()


    try:
        file = open(f'{UMname}', "x")
    except FileExistsError:
        file = open(f'{UMname}', "w")

    count = {}
    for w in text:
            count[w] = 1
            file.write(f'{w} {1} \n'.format(w,1))
    file.close()
    
    fichier_map= UMname
    return fichier_map

#######################################################################################
def prepare_shuffle(fichier_map):

    #get hostname of machine on which we are
#    hostname = str(sp.run('hostname',shell=True,capture_output=True).stdout)[2:-3]
    hostname = os.uname()[1]
    
    # lire le fichier UM.txt
    words = pd.read_csv(fichier_map, delimiter =' ',header=None).loc[:,0]
    table_hachage = []
    for w in words:
        val_hash = (int(zlib.adler32(bytearray(w, "utf8"))))
        print(str(w) + ' has hash associated ' + str(val_hash))
#        table_hachage.append(val_hash)
        table_hachage.extend([w, val_hash])
        try:
            shuffleprepfilename = f'/tmp/jmaksoud/shuffles/{val_hash}-{hostname}.txt'.format(val_hash,hostname)
            file = open(shuffleprepfilename, "x")

        except FileExistsError:
            shuffleprepfilename = f'/tmp/jmaksoud/shuffles/{val_hash}-{hostname}.txt'.format(val_hash,hostname)
            file = open(shuffleprepfilename, "a")
        
        file.write(f'{w} 1 \n'.format(w),encoding='utf-8')
        file.close()
    
    table_hachage = np.reshape(table_hachage,(int(len(table_hachage)/2),2))                
    return pd.DataFrame(table_hachage) 
        
#######################################################################################

def envoi_shuffle_vers_autre_slave(table_hachage_arg):
    
    machines_list = pd.read_csv('/tmp/jmaksoud/machines.txt',header=None) # liste machine
#    indice_machine = (table_hachage_arg.iloc[:,1] % len(machines_list)).astype(int)
    indice_machine = ((table_hachage_arg.iloc[:,1]).astype(int) % len(machines_list)).astype(int)

#    table = pd.concat([table_hachage_arg,pd.DataFrame(indice_machine),pd.DataFrame(machines_list)],axis=1)
    table = pd.concat([table_hachage_arg,pd.DataFrame(indice_machine),pd.DataFrame(machines_list)],axis=1)

    table.columns=['word','hash','modulo','machine']        
    
    mach_dest = []
    for i in table['modulo']:
        print(table['machine'][i])
        mach_dest.append(table['machine'][i])
    table = pd.concat([table,pd.Series(mach_dest)],axis=1) 
    table.columns = ['word','hash','modulo','machine','machine_dest']


    for i in range(len(table)):
        cmd = f"scp /tmp/jmaksoud/shuffles/{table['hash'][i]}* {table['machine_dest'][i]}:/tmp/jmaksoud/shufflesreceived".format(table['hash'][i],table['machine_dest'][i])
        print(cmd)
        sp.run(cmd,shell=True,capture_output=True)
        
    return table
#######################################################################################
########################################################################################
def prepare_shuffle_et_envoi_vers_autres_machines(fichier_map):

    #get hostname of machine on which we are
    hostname= os.uname()[1]
    # lire le fichier UM.txt
    tmp = pd.read_csv(fichier_map, delimiter =' ',header=None)
    words = tmp.loc[:,0]
    counts = tmp.loc[:,1]


    table_hachage = []
    for w in words:
#        print(w)
        val_hash = (int(zlib.adler32(bytearray(w, encoding="utf8"))))
#        print(str(w) + ' has hash associated ' + str(val_hash))
        table_hachage.extend([w, val_hash])
        try:
            shuffleprepfilename = f'/tmp/jmaksoud/shuffles/{val_hash}-{hostname}.txt'.format(val_hash,hostname)
            file = open(shuffleprepfilename, "x")
            file.write(f'{w} 1 \n'.format(w))
            file.close()

        except FileExistsError:
            shuffleprepfilename = f'/tmp/jmaksoud/shuffles/{val_hash}-{hostname}.txt'.format(val_hash,hostname)
            file = open(shuffleprepfilename, "a")
            file.write(f'{w} 1 \n'.format(w))
            file.close()
    
    table_hachage = np.reshape(table_hachage,(int(len(table_hachage)/2),2))                

    machines_list = pd.read_csv('/tmp/jmaksoud/machines.txt',header=None) # liste machine
    indice_machine = ((pd.DataFrame(table_hachage).iloc[:,1]).astype(int) % len(machines_list)).astype(int)
    table = pd.concat([pd.DataFrame(table_hachage),pd.DataFrame(indice_machine),pd.DataFrame(machines_list)],axis=1)
    table.columns=['word','hash','modulo','machine']        
    
    mach_dest = []
    for i in table['modulo']:
        print(table['machine'][i])
        mach_dest.append(table['machine'][i])
    table = pd.concat([table,pd.Series(mach_dest)],axis=1) 
    table.columns = ['word','hash','modulo','machine','machine_dest']
    
    


    for i in range(len(table)):
        cmd = f"scp /tmp/jmaksoud/shuffles/{table['hash'][i]}* {table['machine_dest'][i]}:/tmp/jmaksoud/shufflesreceived".format(table['hash'][i],table['machine_dest'][i])
        print(cmd)
        sp.run(cmd,shell=True,capture_output=True)
           
    return table
        
#        
#######################################################################################
######################################################################################## 
#
#def prepare_shuffle_et_envoi_vers_autres_machines(fichier_map):
#
#    
#    hostname= os.uname()[1] #get hostname of machine on which we are
#    
#
#    words = open(fichier_map,"r", encoding='utf-8').read().replace("\n"," ")#.split()
#    words=re.sub("[^a-zA-Z\d\s:]","",words).split()
#    words = words[::2]
#
#    table_hachage = []
#    for w in words:
#        val_hash = (int(zlib.adler32(bytearray(w, encoding="utf8"))))
##        print(str(w) + ' has hash associated ' + str(val_hash))
#        table_hachage.extend([w, val_hash])
#        try:
#            shuffleprepfilename = f'/tmp/jmaksoud/shuffles/{val_hash}-{hostname}.txt'.format(val_hash,hostname)
#            file = open(shuffleprepfilename, "x")
#            file.write(f'{w} 1 \n'.format(w))
#            file.close()
#
#        except FileExistsError:
#            shuffleprepfilename = f'/tmp/jmaksoud/shuffles/{val_hash}-{hostname}.txt'.format(val_hash,hostname)
#            file = open(shuffleprepfilename, "a")
#            file.write(f'{w} 1 \n'.format(w))
#            file.close()
#    
#    table_hachage = np.reshape(table_hachage,(int(len(table_hachage)/2),2))                
#
#    machines_list = pd.read_csv('/tmp/jmaksoud/machines.txt',header=None) # liste machine
#    indice_machine = ((pd.DataFrame(table_hachage).iloc[:,1]).astype(int) % len(machines_list)).astype(int)
#    machine_dest=[]
#    for k in indice_machine:
#        machine_dest.append(machines_list.iloc[k,0])
#    table = pd.concat([pd.DataFrame(table_hachage),pd.DataFrame(indice_machine),pd.DataFrame(machine_dest)],axis=1)
#    table.columns=['word','hash','modulo','machine_dest']        
#  
#    machines_destination = table['machine_dest'].unique()
#    for machine_dest in machines_destination:
#        table_par_machine_dest=[]
#        table_par_machine_dest = table[table['machine_dest']==machine_dest]
#        zipObj = ZipFile(f"/tmp/jmaksoud/shuffles/{machine_dest}.zip".format(machine_dest), 'w')
#        print(f"/tmp/jmaksoud/shuffles/{machine_dest}.zip".format(machine_dest))
#        for i in table_par_machine_dest.index:
#            nom = f"/tmp/jmaksoud/shuffles/{table_par_machine_dest['hash'][i]}-{table_par_machine_dest['machine_dest'][i]}.txt".format(table_par_machine_dest['hash'][i],table_par_machine_dest['machine_dest'][i])
#            print(nom)
#            zipObj.write(nom)
#        zipObj.close()        


# ce qui est en dessous marche 
#    for i in range(len(table_tmp)):
#        cmd = f"scp /tmp/jmaksoud/shuffles/{table['hash'][i]}* {table['machine_dest'][i]}:/tmp/jmaksoud/shufflesreceived".format(table['hash'][i],table['machine_dest'][i])
#        print(cmd)
#        sp.run(cmd,shell=True,capture_output=True)
           
    return table
        
########################################################################################
####################################################################################### 
    

### !! FONCTION def prepare_shuffle_et_envoi_vers_autres_machines(fichier_map) QUI MARCHE !!
    

#def prepare_shuffle_et_envoi_vers_autres_machines(fichier_map):
#
#    #get hostname of machine on which we are
#    hostname= os.uname()[1]
#    # lire le fichier UM.txt
#    tmp = pd.read_csv(fichier_map, delimiter =' ',header=None)
#    words = tmp.loc[:,0]
#    counts = tmp.loc[:,1]
##    tmp = open(fichier_map,"r", encoding='utf-8').read().replace("\n"," ").split()
##    words=re.sub("[^a-zA-Z\d\s:]","",words).split()
#
#    table_hachage = []
#    for w in words:
##        print(w)
#        val_hash = (int(zlib.adler32(bytearray(w, encoding="utf8"))))
##        print(str(w) + ' has hash associated ' + str(val_hash))
#        table_hachage.extend([w, val_hash])
#        try:
#            shuffleprepfilename = f'/tmp/jmaksoud/shuffles/{val_hash}-{hostname}.txt'.format(val_hash,hostname)
#            file = open(shuffleprepfilename, "x")
#            file.write(f'{w} 1 \n'.format(w))
#            file.close()
#
#        except FileExistsError:
#            shuffleprepfilename = f'/tmp/jmaksoud/shuffles/{val_hash}-{hostname}.txt'.format(val_hash,hostname)
#            file = open(shuffleprepfilename, "a")
#            file.write(f'{w} 1 \n'.format(w))
#            file.close()
#    
#    table_hachage = np.reshape(table_hachage,(int(len(table_hachage)/2),2))                
#
#    machines_list = pd.read_csv('/tmp/jmaksoud/machines.txt',header=None) # liste machine
#    indice_machine = ((pd.DataFrame(table_hachage).iloc[:,1]).astype(int) % len(machines_list)).astype(int)
#    table = pd.concat([pd.DataFrame(table_hachage),pd.DataFrame(indice_machine),pd.DataFrame(machines_list)],axis=1)
#    table.columns=['word','hash','modulo','machine']        
#    
#    mach_dest = []
#    for i in table['modulo']:
#        print(table['machine'][i])
#        mach_dest.append(table['machine'][i])
#    table = pd.concat([table,pd.Series(mach_dest)],axis=1) 
#    table.columns = ['word','hash','modulo','machine','machine_dest']
#
#
#    for i in range(len(table)):
#        cmd = f"scp /tmp/jmaksoud/shuffles/{table['hash'][i]}* {table['machine_dest'][i]}:/tmp/jmaksoud/shufflesreceived".format(table['hash'][i],table['machine_dest'][i])
#        print(cmd)
#        sp.run(cmd,shell=True,capture_output=True)
#           
#    return table
#        
########################################################################################
######################################################################################## 





        
def fonction_reduce():
    #### LECTURE DES FICHIERS SHUFFLESRECEIVED
    text = []
    reduce_count = {}
    path = '/tmp/jmaksoud/shufflesreceived/'
    for filename in glob.glob(os.path.join(path,'*')):
        with open(os.path.join(os.getcwd(), filename), 'r') as f: # open in readonly mode
            text.append(f.read().replace("\n"," ").split()[0])

    #### CONSTRUIRE UN TABLEAU REDUCE_COUNT AVEC MOT, COMPTAGE ET HASH CORRESPONDANT

    for w in text:
        if w not in reduce_count:
            reduce_count[w] = 1 
        else:
            reduce_count[w] += 1
            
        
    reduce_count = pd.DataFrame(list(reduce_count.items()))
    reduce_count.columns=['name','count']    

    val_hash = []
    for w in reduce_count['name']:
        val_hash.append(int(zlib.adler32(bytearray(w, "utf8"))))
        print(str(w) + ' has hash associated ' + str(val_hash))
    
    reduce_count['hash'] = val_hash
   
    #### ECRIRE LE CONTENU DE CE TABLEAU DANS LES FICHIERS QUI IRONT DANS LES DOSSIERS REDUCE

    for w in range(len(reduce_count)):
        try:
            reducefilename = f'/tmp/jmaksoud/reduce/{reduce_count.iloc[w,2]}.txt'.format(reduce_count.iloc[w,2])
            file = open(reducefilename, "x")
            file.write(f'{reduce_count.iloc[w,0]} {reduce_count.iloc[w,1]}'.format(reduce_count.iloc[w,0],reduce_count.iloc[w,1]))
            file.close()
        
        except FileExistsError:
            reducefilename = f'/tmp/jmaksoud/reduce/{val_hash}.txt'.format(reduce_count.iloc[w,2])
            file = open(reducefilename, "a")
            file.write(f'{reduce_count.iloc[w,0]} {reduce_count.iloc[w,1]}'.format(reduce_count.iloc[w,0],reduce_count.iloc[w,1]))
            file.close()

    


#        
if __name__ == '__main__':



    if sys.argv[1] == '0': #map step
        mapping(sys.argv[2])
        
        
    elif sys.argv[1] == '1': #shuffle step
        prepare_shuffle_et_envoi_vers_autres_machines(sys.argv[2])
#
    elif sys.argv[1] == '2': #reduce
        fonction_reduce()



























#
##if len(sys.argv) == 3:
#            if sys.argv[1] == '0':
#                repertoireMaps = '/tmp/vpoquet/maps/'
#                repertoireSplit = '/tmp/vpoquet/splits/'
#                if maps(repertoireSplit, repertoireMaps, sys.argv[2]):
#                    sys.exit(0)
#                else:
#                    sys.exit(1)
#            elif sys.argv[1] == '1':
#                repertoireMaps = '/tmp/vpoquet/maps/'
#                repertoireShuffle = '/tmp/vpoquet/shuffle/'
#                fichier = sys.argv[2]
#                if prepareShuffle(repertoireMaps, repertoireShuffle, fichier):
#                    #print('Préparation terminée')
#                    if shuffle():
#                        sys.exit(0)
#                    else:
#                        sys.exit(1)
#                else:
#                    sys.exit(1)
