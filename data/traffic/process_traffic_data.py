
# coding: utf-8

# In[1]:


import pandas as pd


# In[7]:


root = ''


# In[15]:


def parse_date(date):
    try:
        date_p = pd.to_datetime(date,format="%Y %b %d %I:%M:%S %p", errors='raise')
    except:
        date_p = pd.to_datetime(date,format="%m/%d/%Y %I:%M:%S %p", errors='raise')
    return date_p

#2018 Jul 28 03:28:09 PM


# In[25]:


file = root + 'traffic_speed.csv'
df = pd.read_csv(file, header=0, parse_dates=['DATA_AS_OF'],
                usecols=['DATA_AS_OF','BOROUGH','LINK_POINTS','SPEED','LINK_ID'],
                skipinitialspace=True,
                date_parser = parse_date)
df.info()


# In[26]:


df_link_ids = df[['LINK_ID','LINK_POINTS','BOROUGH']].drop_duplicates()
df_link_ids.info()
df_link_ids.head()


# In[27]:


df_link_ids = df_link_ids.set_index('LINK_ID')
df_link_ids.info()
df_link_ids.head()


# In[28]:


df_link_ids.to_csv(root+'traffic_links.csv')


# In[30]:


df_clean = df[['DATA_AS_OF','LINK_ID','SPEED']].set_index('DATA_AS_OF')
df_clean.head()


# In[31]:


df_clean = df_clean.loc['2016-01-01':'2017-12-31']
df_clean.info()


# In[32]:


df_clean = df_clean.groupby('LINK_ID')['SPEED'].resample('1H').mean()


# In[34]:


df_clean.head()


# In[35]:


df_clean.to_csv(root+'traffic_1617_hourly.csv')

