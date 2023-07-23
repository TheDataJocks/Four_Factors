from bs4 import BeautifulSoup
import requests
import pandas as pd
import difflib
import time
import numpy as np
from scipy.stats import percentileofscore
from scipy.stats import linregress



def create_total_score_sheet():
    #main function that runs to create the rankings.
    #before running, run "get_overall_dataset", get_by_game_stats", and "get_draft_date" with save=True
    links = get_player_links()
    consistency = consistency_score(links)
    trend = trend_score(links)
    names = get_player_by_link(links)
    consensus = consensus_score(names)
    pedigree = pedigree_score(names)
    df = pd.DataFrame({'Player':names,'Consensus':consensus,'Pedigree':pedigree,'Consistency':consistency,'Trend':trend})
    df.sort_values(['Consensus','Pedigree','Trend','Consistency'],ascending=False,inplace=True)
    return(df)

def get_overall_dataset(save=False):
    #For each of last 2 years, grab the top 200, their VBD, and their link
    try:
        frame= pd.read_csv('by_year_21_22.csv')
    except FileNotFoundError:
        page22 = requests.get('https://www.pro-football-reference.com/years/2022/fantasy.htm')
        page21 = requests.get('https://www.pro-football-reference.com/years/2021/fantasy.htm')

        soup22=BeautifulSoup(page22.content)
        soup21=BeautifulSoup(page21.content)
        #names, remove * and +
        player_cells22 = soup22.findAll('td',{'data-stat':'player'})
        player_names22=[cell.get_text().translate({ord(i): None for i in '*+'}) for cell in player_cells22[0:200]]
        player_links22 = [cell.find('a').get('href') for cell in player_cells22[0:200]]
        
        player_cells21 = soup21.findAll('td',{'data-stat':'player'})
        player_names21=[cell.get_text().translate({ord(i): None for i in '*+'}) for cell in player_cells21[0:200]]
        player_links21 = [cell.find('a').get('href') for cell in player_cells21[0:200]]
    
        posrk22_cells = soup22.findAll('td',{'data-stat':'fantasy_rank_pos'})
        posrk22 = [cell.get_text() for cell in posrk22_cells[0:200]]
        posrk21_cells = soup22.findAll('td',{'data-stat':'fantasy_rank_pos'})
        posrk21 = [cell.get_text() for cell in posrk21_cells[0:200]]
        
        pos22_cells = soup22.findAll('td',{'data-stat':'fantasy_pos'})
        pos22 = [cell.get_text() for cell in pos22_cells[0:200]]
        pos21_cells = soup22.findAll('td',{'data-stat':'fantasy_pos'})
        pos21 = [cell.get_text() for cell in pos21_cells[0:200]]
        
        player_names = player_names21+player_names22
        player_links = player_links21+player_links22
        posrk = posrk21+posrk22
        pos = pos21+pos22
        frame = pd.DataFrame({'Name':player_names,'Pos':pos,'Link':player_links,'Pos_Rk':posrk,'Year':[2021 for i in range(200)]+[2022 for i in range(200)]})
    if save:
        frame.to_csv('by_year_21_22.csv',index=False) 
    return(frame)

def get_player_links():
    try:
        frame= pd.read_csv('by_year_21_22.csv')
    except FileNotFoundError:
        frame = get_overall_dataset()
    links=list(set(list(frame["Link"])))
    return(links)

def get_by_game_stats(save=False):
    #For each player link provided, grab their game by game (a) fantasy pts and (b) % snaps played
    #game logs are stored as lists of form pts_game1;pts_game2;pts_game3
    links=get_player_links()
    nrows = len(links)
    pts22 = [0 for i in range(nrows)]
    pts21 = [0 for i in range(nrows)]
    pct_snaps22 = [0 for i in range(nrows)]
    pct_snaps21 = [0 for i in range(nrows)]
    for i in range(nrows):
        #no more than 20 requests a minute! THrottle to 15 out of respect
        time.sleep(4)
        print("Fetching Data for Player " + str(i) + ' of ' + str(nrows))
        #the [0:-4] removes the .htm at the end of the links, can do cleaner
        url22 = 'https://www.pro-football-reference.com'+links[i].replace('.htm','')+'/fantasy/2022/'

        page22 = requests.get(url22)
        soup22 = BeautifulSoup(page22.content)
        pts_cells22 = soup22.findAll('td',{'data-stat':'fantasy_points'})
        snap_cells22 = soup22.findAll('td',{'data-stat':'off_pct'})
        pts22[i] = ';'.join([cell.get_text() for cell in pts_cells22])
        pct_snaps22[i] = ';'.join([cell.get_text() for cell in snap_cells22])
        
        time.sleep(4 )
        url21 = 'https://www.pro-football-reference.com'+links[i].replace('.htm','')+'/fantasy/2021/'
        page21 = requests.get(url21)
        soup21 = BeautifulSoup(page21.content)
        pts_cells21 = soup21.findAll('td',{'data-stat':'fantasy_points'})
        snap_cells21 = soup21.findAll('td',{'data-stat':'off_pct'})
        pts21[i] = ';'.join([cell.get_text() for cell in pts_cells21])
        pct_snaps21[i] = ';'.join([cell.get_text() for cell in snap_cells21])
    
    df = pd.DataFrame({'Link':links,'PTS21':pts21,'PTS22':pts22,'SNAP_PCT21':pct_snaps21,'SNAP_PCT22':pct_snaps22})
    if save:
        df.to_csv('by_game_stats_21_22.csv',index=False)
    return(df)
    
def get_draft_data(save=False):
    #get the draft order from the last two drafts
    try:
        df=pd.read_csv('drafts_22_and_23.csv')
    except FileNotFoundError:
        page23 = requests.get('https://www.pro-football-reference.com/years/2023/draft.htm')
        soup23 = BeautifulSoup(page23.content)
        link_cells23=soup23.findAll('td',{'data-stat':'player'})
        pick_cells23=soup23.findAll('td',{'data-stat':'draft_pick'})
        pos_cells23 = soup23.findAll('td',{'data-stat':'pos'})
        links23=[cell.find('a').get('href') for cell in link_cells23]
        picks23=[cell.get_text() for cell in pick_cells23]
        names23=[cell.get_text() for cell in link_cells23]
        pos23=[cell.get_text() for cell in pos_cells23]
        yr23 = [2023 for cell in link_cells23]
        
        page22 = requests.get('https://www.pro-football-reference.com/years/2022/draft.htm')
        soup22 = BeautifulSoup(page22.content)
        link_cells22=soup22.findAll('td',{'data-stat':'player'})
        pick_cells22=soup22.findAll('td',{'data-stat':'draft_pick'})
        pos_cells22 = soup22.findAll('td',{'data-stat':'pos'})
        links22=[cell.find('a').get('href') for cell in link_cells22]
        picks22=[cell.get_text() for cell in pick_cells22]
        names22=[cell.get_text() for cell in link_cells22]
        pos22=[cell.get_text() for cell in pos_cells22]
        yr22 = [2022 for cell in link_cells22]

        df = pd.DataFrame({'Links':links23+links22,'Picks':picks23+picks22,'Name':names23+names22,'Year':yr23+yr22,'Pos':pos23+pos22})
        if save:
            df.to_csv('drafts_22_23.csv',index=False)
    return(df)

def consensus_score(player_list):
    #For every player given in the list, compute their expert consensus score /10.
    # a "last starter" is a 6, interpolate linearly so RK1==10; cap to [1,10], round to 0.5
    df = pd.read_csv('consensus_23.csv')
    consensus_scores = [0 for i in range(len(player_list))]
    for i in range(len(player_list)):
        try:
            name_in_df = difflib.get_close_matches(player_list[i],df['PLAYER NAME'],1)[0]
        except IndexError:
            consensus_scores[i]=0
            continue
        if name_in_df is None:
            consensus_scores[i]=0
        else:
            #find position and rk within position
            pos_entry = df["POS"][df["PLAYER NAME"]==name_in_df].values[0]
            pos = "".join(c for c in pos_entry if c.isalpha())
            rk = int("".join(c for c in pos_entry if c.isnumeric()))
            if pos=='RB':
                #24 starters; 23 spots = 4 pts
                raw_score = min(max(0,(24-rk)*(4/23)+6),10)
                consensus_scores[i]= round(raw_score*2)/2 
            elif pos=='WR':
                #30 starters; 29 spots = 4 pts
                raw_score = min(max(0,(30-rk)*(4/29)+6),10)
                consensus_scores[i]= round(raw_score*2)/2 
            elif pos=='TE':
                #24 starters; 23 spots = 4 pts
                raw_score = min(max(0,(12-rk)*(4/11)+6),10)
                consensus_scores[i]= round(raw_score*2)/2 
            elif pos=='QB':
                #24 starters; 23 spots = 4 pts
                raw_score = min(max(0,(12-rk)*(4/11)+6),10)
                consensus_scores[i]= round(raw_score*2)/2 
            else:
                consensus_scores[i]=0
    return(consensus_scores)

def consistency_score(link_list):
    #two parts
    #1) over last 2 seasons, variance/mean^2 
    #2) over the last 2 seasons, what % of games did they miss?
    #for rookies and sophomores, regress to the mean
    #keep_default_na makes empty cells read as ''
    data = pd.read_csv('by_game_stats_21_22.csv',keep_default_na=False)
    nplayers = len(link_list)
    metric1_21 = [0 for i in range(nplayers)]
    metric2_21 = [0 for i in range(nplayers)]
    metric1_22 = [0 for i in range(nplayers)]
    metric2_22 = [0 for i in range(nplayers)]
    consistency_score=[0 for i in range(nplayers)]
    for i in range(nplayers):
        if len(data["PTS21"].iloc[i])>0:
            pts21 = [float(x or 0) for x in data["PTS21"].iloc[i].split(';')[0:-1]]
            #subtract variance, bigger is better, also weights people who score more 
            m=max(pts21)
            if m==0:
                metric1_21[i]=0
            else:
                metric1_21[i] = np.sum([x/m for x in pts21])
        else:
            metric1_21[i]=np.nan
        if len(data["PTS22"].iloc[i])>0:
            pts22 = [float(x or 0) for x in data["PTS22"].iloc[i].split(';')][0:-1]
            m=max(pts22)
            if m==0:
                metric1_22[i]=0
            else:
                metric1_22[i]=np.sum([x/m for x in pts22])
        else:
            metric1_22[i]=np.nan
        if len(data["SNAP_PCT21"].iloc[i])>0:
            snap_pct21 = [float(x.replace('%','') or 0) for x in data["SNAP_PCT21"].iloc[i].split(';')[0:-1]]
            metric2_21[i] = np.sum(snap_pct21)
        else:
            snap_pct21 = np.nan
        if len(data["SNAP_PCT22"].iloc[i])>0:
            snap_pct21 = [float(x.replace('%','') or 0) for x in data["SNAP_PCT22"].iloc[i].split(';')[0:-1]]
            metric2_22[i] = np.sum(snap_pct21)
        else:
            metric2_22[i]=np.nan
    #now, return percentiles
    for i in range(nplayers):
        pctile1= percentileofscore(metric1_21,metric1_21[i],nan_policy='omit')
        pctile2= percentileofscore(metric1_22,metric1_22[i],nan_policy='omit')
        pctile3= percentileofscore(metric2_21,metric2_21[i],nan_policy='omit')
        pctile4= percentileofscore(metric2_22,metric2_22[i],nan_policy='omit')
        consistency_score[i] = round(np.nanmean([pctile1,pctile2,pctile3,pctile4])/5)/2
    return(consistency_score)


def trend_score(link_list):
    #compute a player's opportunity score
    data = pd.read_csv('by_game_stats_21_22.csv',keep_default_na=False)
    nplayers = len(data["Link"])
    raw_trend_scores=[0 for i in range(nplayers)]
    trend_scores = [0 for i in range(nplayers)]
    for i in range(nplayers):
        pts21=data["PTS21"].iloc[i]
        pts22=data["PTS22"].iloc[i]
        if len(pts21)==0:
            pts21=[]
        else:
            pts21 = [float(x or 0) for x in pts21.split(';')[0:-1]]
        if len(pts22)==0:
            pts22=[]
        else:
            pts22 = [float(x or 0) for x in pts22.split(';')[0:-1]]
        pts=pts21+pts22
        if len(pts)<=3:
            raw_trend_scores[i]=np.nan
        else:
            model = linregress(range(len(pts)),pts)
            raw_trend_scores[i]=model.slope
    for i in range(nplayers):
        trend_scores[i]=round(percentileofscore(raw_trend_scores,raw_trend_scores[i])/5)/2
    return(trend_scores)
        
def pedigree_score(player_list):
    #compute a player's pedigree score:
    stats_df = pd.read_csv('by_year_21_22.csv')
    draft_df = pd.read_csv('drafts_22_23.csv')
    pedigree = [0 for i in range(len(player_list))]
    for i in range(len(player_list)):
        player =player_list[i]
        #see how old they are
        subdraft = draft_df[draft_df["Name"]==player]
        if len(subdraft)==1:
            #check if rookie or sophomore
            if subdraft['Year'].iloc[0]==2023:
                #rookie
                #pedigree changes based on qb or not
                if subdraft["Pos"].iloc[0]=='QB':
                    #complicated
                    if subdraft["Picks"].iloc[0]==1:
                        pedigree[i]=10;
                    elif subdraft["Picks"].iloc[0] ==3:
                        pedigree[i] = 9.5
                    elif subdraft["Picks"].iloc[0]==3:
                        pedigree[i]=9
                    elif subdraft["Picks"].iloc[0]<=10:
                        pedigree[i]=8
                    elif subdraft["Picks"].iloc[0]<=20:
                        pedigree[i] =7
                    elif subdraft["Picks"].iloc[0]<=30:
                        pedigree[i] =6
                    elif subdraft["Picks"].iloc[0]<=60:
                        pedigree[i] = 3
                else:
                    #not a QB. Top 5=10
                    if subdraft["Picks"].iloc[0]<=5:
                        pedigree[i]=10
                    elif subdraft["Picks"].iloc[0]<=60:
                        #pick 60== rating 6, pick 6 = 9.5
                        raw_rank = 9.5-(float(subdraft["Picks"].iloc[0])-6)*3.5/54
                        pedigree[i] = round(raw_rank*2)/2 #round to nearest half
                    else:
                        #pick 61=6, pick 200=1
                        raw_rank = max(6-(float(subdraft["Picks"].iloc[0])-61)*5/139,0)
                        pedigree[i]=round(raw_rank*2)/2
            elif subdraft['Year'].iloc[0]==2022:
                #sophomore
                #first compute draft capital
                if subdraft["Pos"].iloc[0]=='QB':
                    #complicated
                    if subdraft["Picks"].iloc[0]==1:
                        draft_capital=10;
                    elif subdraft["Picks"].iloc[0] ==3:
                        draft_capital = 9.5
                    elif subdraft["Picks"].iloc[0]==3:
                        draft_capital=9
                    elif subdraft["Picks"].iloc[0]<=10:
                        draft_capital=8
                    elif subdraft["Picks"].iloc[0]<=20:
                        draft_capital =7
                    elif subdraft["Picks"].iloc[0]<=30:
                        draft_capital =6
                    elif subdraft["Picks"].iloc[0]<=60:
                        draft_capital = 3
                else:
                    #not a QB. Top 5=10
                    if subdraft["Picks"].iloc[0]<=5:
                        draft_capital=10
                    elif subdraft["Picks"].iloc[0]<=60:
                        #pick 60== rating 6, pick 6 = 9.5
                        raw_rank = 9.5-(float(subdraft["Picks"].iloc[0])-6)*3.5/54
                        draft_capital = round(raw_rank*2)/2 #round to nearest half
                    else:
                        #pick 61=6, pick 200=1
                        raw_rank = max(6-(float(subdraft["Picks"].iloc[0])-61)*5/139,0)
                        draft_capital=round(raw_rank*2)/2
                #now compute production score
                #use overall rk
                sub_prod = stats_df[stats_df["Name"]==player]
                # Compute overall rk by finding their position rank
                if sub_prod["Pos"].iloc[0]=='QB':
                    prod_score = round(max(6+(float(12-sub_prod["Pos_Rk"].iloc[0]))*4/11,0)*2)/2
                elif sub_prod["Pos"].iloc[0]=='TE':
                    prod_score = round(max(6+(float(12-sub_prod["Pos_Rk"].iloc[0]))*4/11,0)*2)/2
                elif sub_prod["Pos"].iloc[0]=='RB':
                    prod_score = round(max(6+(float(24-sub_prod["Pos_Rk"].iloc[0]))*4/23,0)*2)/2
                elif sub_prod["Pos"].iloc[0]=='WR':
                    prod_score = round(max(6+(float(30-sub_prod["Pos_Rk"].iloc[0]))*4/29,0)*2)/2
                pedigree[i]=round(max(prod_score,draft_capital)*2)/2
        else:
                #veteran. Check their last 2 seasons and use the better of the two.
                #if didn't play, their pedigree is 0
                sub_prod = stats_df[stats_df["Name"]==player]
                #if they have 2 seasons, use the better of the two
                prod_score = 0
                for j in range(len(sub_prod)):
                    if sub_prod["Pos"].iloc[j]=='QB':
                        raw_prod_score = round(max(6+(float(12-sub_prod["Pos_Rk"].iloc[j]))*4/11,0)*2)/2
                    elif sub_prod["Pos"].iloc[j]=='TE':
                        raw_prod_score = round(max(6+(float(12-sub_prod["Pos_Rk"].iloc[j]))*4/11,0)*2)/2
                    elif sub_prod["Pos"].iloc[j]=='RB':
                        raw_prod_score = round(max(6+(float(24-sub_prod["Pos_Rk"].iloc[j]))*4/23,0)*2)/2
                    elif sub_prod["Pos"].iloc[j]=='WR':
                        raw_prod_score = round(max(6+(float(30-sub_prod["Pos_Rk"].iloc[j]))*4/29,0)*2)/2
                    prod_score = max(prod_score,raw_prod_score)
                pedigree[i]=prod_score
    return(pedigree)
        
def get_player_by_link(link_list):
    #return a list of names corresponding to a list of links
    data=pd.read_csv('by_year_21_22.csv') 
    rookies = pd.read_csv('drafts_22_23.csv')
    names =['' for i in range(len(link_list))]
    for i in range(len(link_list)):
        try:
            names[i]=data[data["Link"]==link_list[i]]["Name"].iloc[0]
        except IndexError:
            #they must be a rookie
            names[i]=rookies[rookies["Link"]==link_list[i]]["Name"].iloc[0]
    return(names)
    
            
                
            

    

