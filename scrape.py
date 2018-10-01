import re
import sys
import urllib.request as request, urllib.error as error
from bs4 import BeautifulSoup

def getYears(start,end):
    years = []
    for x in range(start,end):
        years.append(str(x))
    return years

################################################ METHODS FOR The College STATS DATA ################################################
'''
    * We have about 40,364  nodes in the college data in total 
    * Pulls in the collage data for Passing, Rushing and Receiving 
    * The years are from 1956 until 2017 TODO may have to drop some years to align with NFL data
    
    * Table structure Passing :
    Rnk | Player Name | School | Conf | Games Played | Cmp (passing) | Att (passing) | Percent (passing) | Yards (passing) | Avg Yards (passing) | 
    TD (passing) | Interceptins (passing) | Rate (passing) | Att (rushing) | Yds (rushing) | Avg Yards (rushing) | TD (rushing) | Year |
    
    * Table structure Rushing :
    Rnk | Player | School | Conf | Games Played | Att (rushing) | Yds (rushing) | Avg (rushing) | TD (rushing) | Rec (receiving) | Yds (receiving) |
    Avg (receiving) | TD (receiving) | Plays (scrimmage)| Yds (scrimmage) | Avg (scrimmage) | TD (scrimmage) | Year |

    * Table structure Receiving :
    Rnk | Player | School | Conf | Games Played | Rec (receiving) | Yds (receiving) | Avg (receiving) | TD (receiving) |  Att (rushing) | Yds (rushing) | 
    Avg (rushing) | TD (rushing) | Plays (scrimmage)| Yds (scrimmage) | Avg (scrimmage) | TD (scrimmage) | Year |

'''
def ScrapeCollegeStats(years,dataTypes):
    RushingPlayers = [] # Holds the data for the running backs
    PassingPlayers = [] # Holds the data for the QB's
    ReceivingPlayers = [] # Holds the data for Recviers 

    for year in years:
        print("Getting College data for " + year + ":")
        for infoType in dataTypes: 
            if (infoType == "Rushing"):
                url = "https://www.sports-reference.com/cfb/years/"+year+"-rushing.html"
                print(" *Rushing")
            elif (infoType == "Passing"):
                url = "https://www.sports-reference.com/cfb/years/"+year+"-passing.html"
                print(" *Passing")
            elif (infoType == "Receiving"):
                url = "https://www.sports-reference.com/cfb/years/"+year+"-receiving.html"
                print(" *Receiving")

            try:
                page = request.urlopen(url)
            except HTTPError as e: 
                print(e)
            except URLError:
                print("The url didn't work the server could be down or the domain no longer exists ")
            else: 
                soup = BeautifulSoup(page, 'html.parser')

                # Get the data from the table 
                if (infoType == "Rushing"):
                    table = soup.find('table', attrs={'id': 'rushing'})
                elif (infoType == "Passing"):
                    table = soup.find('table', attrs={'id': 'passing'})
                elif (infoType == "Receiving"):
                    table = soup.find('table', attrs={'id': 'receiving'})

                tableBody = table.find('tbody').select("tr")

                for row in tableBody :
                    text = row.getText(",")
                    if(text[0].isdigit()): # Makes sure we don't get any table headers in the data
                        text += "," + year
                        text = text.replace(",*,", ",") # cleans the data form something we scrapped 
                        if (infoType == "Rushing"):
                            RushingPlayers.append(text)
                        elif (infoType == "Passing"):
                            PassingPlayers.append(text)
                        elif (infoType == "Receiving"):
                            ReceivingPlayers.append(text)

    # print("\n".join(RushingPlayers))
    # print("\n".join(PassingPlayers))
    # print(" \n".join(ReceivingPlayers))
    # totalPlayers = len(RushingPlayers) + len(PassingPlayers)  + len(ReceivingPlayers)
    # print("Total " + str(totalPlayers))
    return (RushingPlayers,PassingPlayers,ReceivingPlayers)

################################################ METHODS FOR THE NFL DATA ################################################
'''
    * Number of Players in total (1937 - 2017) 24,788
    * Pulls in the NFL data for passing yards 
    * The years are from 1937 until 2017
    
    * Table structure Passing :
    Rnd | Pick | Team | Player Name | Pos | Age | To (year they play to) | AP1 | PB | ST | CarAV | DrAV | G | Cmp (passing) | Att (passing) | Tds (passing) |
    Int (passing) | Att (rushing) | Yds (rushing) | TD (rushing) | Rec (receiving) | Yds (receiving) | TD (receiving) | Tkl | Int | Sk | College | Draft Year

'''

def ScrapeNflDraftData(years):
    Players = []

    for year in years:
        print("Getting NFL data for " + year + ":")
        url = "https://www.pro-football-reference.com/years/"+year+"/draft.htm"
        try:
            page = request.urlopen(url)
        except HTTPError as e: 
            print(e)
        except URLError:
            print("The url didn't work the server could be down or the domain no longer exists ")
        else: 
            soup = BeautifulSoup(page, 'html.parser')

            # Get the data from the table 
            table = soup.find('table', attrs={'id': 'drafts'})

            tableBody = table.find('tbody').select("tr")

            for row in tableBody :
                text = row.getText(",")
                if(text[0].isdigit()): # Makes sure we don't get any table headers in the data
                    text += "," + year
                    text = text.replace(",College Stats", "") # cleans the data form something we scrapped 
                    Players.append(text)

    # print("\n".join(Players))
    # print("Number of drafted Players " + str(len(Players)))
    return Players


################################################ METHODS FOR THE COMBINE DATA ################################################
'''
    * Number of Node 5,433 2000 - 2017
    * Pulls in the NFL Draft data for all players  
    * The years are from 2000 until 2017
    
    * Table structure Passing :
    Rk | Year | Player | Pos | AV | School | College | Height | Wt | 40yd | Vertical | BenchReps | Broad Jump | 3Cone | Shuttle | Drafted (Tm / Rd/ Yr) |

'''

def ScrapeCombineData(years,dataTypes):
    OffensePlayers = [] # Holds the data for Offense
    DefensePlayers = [] # Holds the data for Defense
    SpecailPlayers = [] # Holds the data for Special Teams  

    for year in years:
        print("Getting Combine data for " + year + ":")
        for infoType in dataTypes: 
            if (infoType == "Offense"):
                url = "https://www.pro-football-reference.com/play-index/nfl-combine-results.cgi?request=1&year_min="+year+"&year_max="+year+"&pos%5B%5D=QB&pos%5B%5D=WR&"
                url += "pos%5B%5D=TE&pos%5B%5D=RB&pos%5B%5D=FB&pos%5B%5D=OT&pos%5B%5D=OG&pos%5B%5D=C&show=all&order_by=year_id"
                print(" *Offense")
            elif (infoType == "Defense"):
                url = "https://www.pro-football-reference.com/play-index/nfl-combine-results.cgi?request=1&year_min="+year+"&year_max="+year+"&pos%5B%5D=DE&pos%5B%5D=DT"
                url += "&pos%5B%5D=EDGE&pos%5B%5D=ILB&pos%5B%5D=OLB&pos%5B%5D=SS&pos%5B%5D=FS&pos%5B%5D=S&pos%5B%5D=CB&show=all&order_by=year_id"
                print(" *Defense")
            elif (infoType == "Special"):
                url = "https://www.pro-football-reference.com/play-index/nfl-combine-results.cgi?request=1&year_min="+year+"&year_max="+year 
                url += "&pos%5B%5D=LS&pos%5B%5D=K&pos%5B%5D=P&show=all&order_by=year_id"
                print(" *Special")
            try:
                page = request.urlopen(url)
            except HTTPError as e: 
                print(e)
            except URLError:
                print("The url didn't work the server could be down or the domain no longer exists ")
            else: 
                soup = BeautifulSoup(page, 'html.parser')

                # Get the data from the table 
                table = soup.find('table', attrs={'id': 'results'})
                tableBody = table.find('tbody').select("tr")

                for row in tableBody :
                    text = row.getText(",")
                    if(text[0].isdigit()): # Makes sure we don't get any table headers in the data
                        if (infoType == "Offense"):
                            OffensePlayers.append(text)
                        elif (infoType == "Defense"):
                            DefensePlayers.append(text)
                        elif (infoType == "Special"):
                            SpecailPlayers.append(text)

    # print("\n".join(OffensePlayers))
    # print("\n".join(DefensePlayers))
    # print("\n".join(SpecailPlayers))
    # numberOFNodes = len(OffensePlayers)  + len(DefensePlayers) + len(SpecailPlayers)
    # print("Total "+ str(numberOFNodes))
    return (OffensePlayers,DefensePlayers,SpecailPlayers)