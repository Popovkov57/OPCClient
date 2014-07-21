#!/usr/bin/python
# -*- coding: utf-8 -*-

import OpenOPC
from Tkinter import *
import sqlite3
import os
import MySQLdb
import mysql.connector
import time

def connect(serveurName, taglist):
    """
    Connexion serveur OPC
    serveurName: name of the OPC server
    taglist: list with name of item
    return list of item information
    """      
    try:
        # Connexion serveur
        opc = OpenOPC.client()
        u= opc.connect(serveurName)
	# Read item list
	v = opc.read(taglist)
	# Close socket
	opc.close()
    except:
        print "Error - No connection to OPC server !!!"
	
    return v


def printItems(taglist,liste):
    """
    Extract values of all items
    taglist: list with name of all items
    liste: list of item value
    return values of all items in a list
    """
    v = connect('***',taglist)
    # *** is the name of the OPC Server 
		
    # Stock item value in liste
    for i in range(len(v)):

        (name, val, qual, time) = v[i]
        if val == '':
            val = 0
        liste.append(val)
        
    return liste

def printItem(taglist,liste,position):
    """
    Extract a value of an item 
    taglist: list with name of all items
    liste: list of item value
    position: position of the item in liste
    return value of an item
    """
    v = connect('***',taglist)
        
    # Stockage des Items dans la liste
    for i in range(len(v)):
        (name, val, qual, time) = v[i]
        if val == '':
            val = 0
        liste.append(val)
    resultat = liste[position]
    
    return resultat

        
def demandValue(taglist,liste,phrase1,mot):
    """
    put values of all items in string to insert in database
    taglist: list with name of all items
    liste: list of item value
    phrase1: string with values of all items
    mot: value of an item
    return a string with values of all items separated with ','
    """
    for i in range(len(liste)):
        mot = str(liste[i])
        phrase1 += "'"
        phrase1 += mot
        phrase1 += "'"
        if not i == len(liste)-1:
            phrase1 += ','
            
    return phrase1

def demandNameItem(listDb,phrase2,mot):
    """
    put database name of all items in string to insert in database
    listDb: list with datbase name of all items
    phrase2: string with database name of all items
    mot: database name of an item
    return a string with database name of all items separated with ','
    """
    for i in range(len(listDb)):
        mot = str(listDb[i])
        phrase2 += mot
        if not i == len(listDb)-1:
            phrase2 += ','

    return phrase2


def preparRequete(phrase1,phrase2):

    """
    put database request in a string 
    phrase1: string with values of all items
    phrase1: string with database name of all items
    return final request to insert values of all item in the database
    """

    requete = ''
    requeteDebut = 'INSERT INTO tablename('
    requeteFin = ') VALUES('
    requete = requeteDebut + phrase2 + requeteFin + phrase1
    requete += ')'
    
    return requete


print("")
print("------------- CLIENT OPC PYTHON  -------------\n")
print("")
print("Menu:\n")
print("1. Start ")
print("2. Stop ")
print("")

# ... is the name of item
taglist =["....","...."]

# datebase name of item
listDb =["time"
          ,"..."
          ,"..."]


liste = []
phrase1 = ''
phrase2 = ''
mot = ''
i = 0
positionItem = ** # replace ** with a position  
resultat = ''
Quit = 0
test_BDD = 0


while Quit != 1 :
	try:
            print "Menu:"
	    nb = input()

            # to start program	
	    if nb == 1:

                print "Connexion to OPC server"

                # Value of an item at time = t 
                resultat1 = printItem(taglist,liste,positionItem)

                # infite loop to test if there a value of a item change
                while 1:

                    test_BDD = 0

                    time.sleep(20) # Pause of 20 sec
                    
                    temps = time.strftime("%H:%M:%S") # Real time

                    # Value of an item at time = t+1
                    resultat2 = printItem(taglist,liste,positionItem)

                    # if value change, it will save the new value in the database
                    if(resultat1 != resultat2):

                        print "Receive OPC data at ", temps

                        # Erase the list
                        phrase1 = ''
                        phrase2 = ''
                        del liste[:]
                        
                        # Preparation of the database request
                        phrase1 = demandValue(taglist,liste,phrase1,mot)
                        phrase2 = demandNameItem(listDb,phrase2,mot)
                        query = preparRequete(phrase1,phrase2)

                        i = 0

                        # Try max 3 times to insert data in the database 
                        while test_BDD != 1 and i <= 3: 

                                if i == 3:
                                    i = 3
                                    print "Connexion to BDD impossible at ", temps, "\n"

                                
                                test_BDD = 0

                                # print "Boucle 2 at ", temps
                                
                                try:
                                    if i <= 2:
                                        print "Send data to database at ", temps
                                    # Connexion to the database don't forget to enter personal information
                                    cnx = mysql.connector.connect(user='...',
                                                                  password='',
                                                                  host='127.0.0.1',
                                                                  database='...')
                                    
                                    # Send data in database
                                    cur = cnx.cursor()
                                    cur.execute(query)
                                    cnx.commit()
                                    cnx.close()

                                    # to exit the loop
                                    test_BDD = 1
                                    
                                except:
                                    time.sleep(5)
                                    if i <= 2:
                                        print " -------------------------- "
                                        print " Error - connexion database"
                                        print " -------------------------- "

                                i += 1
                        
                        # Erase the list
                        phrase1 = ''
                        phrase2 = ''
                        del liste[:]

                        resultat1 = printItem(taglist,liste,positionRoll)

                        if i == 4:
                            break


                    resultat2 = ''  
                    del liste[:]

	    elif nb == 2:
                # to stop program
		Quit = 1
	    
	except:
	    print "Error - Client is disconnected "
