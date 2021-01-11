#/usr/local/lib python3
# coding: utf-8

import pyarrow
import pandas as pd
from datetime import date, datetime

from google.cloud import bigquery as bq

from matplotlib import pyplot as plt
from matplotlib import font_manager as fm, rcParams
import matplotlib.ticker as ticker
import matplotlib.dates as mdates

import theme
import sys

FILE_PATH = sys.path[0]

## Initialize a BigQuery client connection
## Uses previously-set environment variables to pass in credentials.
bq_client = bq.Client()


## Read in query to parse JSON objects in `data` column. 
with open(f'{FILE_PATH}/curated_users_query.sql', 'r') as file:
    sql_query = file.read()

# Run the query, return as Pandas dataframe
daily_acquisition_df = bq_client.query(sql_query).to_dataframe()

# Save dataframe as csv to create up-to-date archive.
daily_acquisition_df.to_csv(f"{FILE_PATH}/data/daily-users.csv")

# Set style for plot
## This is an easy shortcut to implement some nice pre-set styling. We'll customize it from here.
plt.style.use('fivethirtyeight')
font_prop = fm.FontProperties(fname=f'{FILE_PATH}/../assets/fonts/Gordita-Medium.otf')
font_prop_light = fm.FontProperties(fname=f'{FILE_PATH}/../assets/fonts/Gordita-Regular.otf')

# Create the plot
fig, ax = plt.subplots(figsize=(18,12))

ax.plot(daily_acquisition_df.date, 
        daily_acquisition_df.cumulative_users,
        color=theme.brand_primary,
        lw=6,
        ls='-',
        alpha=.9)

ax.set_ylim(-10, max(ax.lines[0].get_ydata())*1.3)
ax.grid(ls='--')
ax.set_title('Total Users', fontsize=35, weight='bold', color=theme.brand_primary, fontproperties=font_prop)
ax.set_ylabel('Number of Users', fontproperties=font_prop_light, fontsize=20)
ax.set_xlabel('Acquisition Date', fontproperties=font_prop_light, fontsize=20)
ax.xaxis.labelpad = 20
ax.yaxis.labelpad = 20
ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
ax.yaxis.set_major_formatter(ticker.StrMethodFormatter('{x:,.0f}'))

xdata = list(ax.lines[0].get_xdata())
ydata = list(ax.lines[0].get_ydata())

# Add an annotation to the last point on the line to note the value and monthly change
ax.annotate('{0}: {1:,.0f} users\n{2:.0%} MoM growth ({3} new users)'.format(xdata[-1].strftime('%B %d, %Y'),
                                                                             ydata[-1],
                                                                             (ydata[-1]-ydata[-31])/ydata[-31],
                                                                             int(ydata[-1]-ydata[-31])),
            xy=(xdata[-1],
                ydata[-1]),
            xytext=(-15,0),
            textcoords='offset points', 
            ha='right', 
            fontsize=20,
            fontproperties=font_prop_light)

# Add a marker to the last point to emphasize it
ax.plot(xdata[-1],
        ydata[-1], 
        marker='o', 
        markersize=15,
        markerfacecolor=theme.white, 
        markeredgecolor=theme.brand_primary,
        markeredgewidth=2.5)


## Annotate a few points along the line to call out big events (soft launch, public launch, etc.)
# Draw a marker
date_dict = {date(2019,11,20): 'Public Launch', 
             date(2018,7,29): 'Beta Launch'}

for d in date_dict.keys():
    ax.plot(d,
            ydata[xdata.index(d)], # Use the index of the date in the X series to find the corresponding Y value.
            marker='o', 
            markersize=15,
            markerfacecolor=theme.white, 
            markeredgecolor=theme.brand_primary,
            markeredgewidth=2.5)

    # Annotate the point
    ax.annotate(f"{d.strftime('%B %d, %Y')}:\n{date_dict[d]}",
                xy=(d,ydata[xdata.index(d)]),
                xytext=(-7,7),
                textcoords='offset points',
                ha='right', 
                fontsize=20,
                fontproperties=font_prop_light)


for label in ax.get_xticklabels():
    label.set_fontproperties(font_prop_light)

for label in ax.get_yticklabels():
    label.set_fontproperties(font_prop_light)

plt.savefig("{}/src_images/total_user_acquisition".format(FILE_PATH), bbox_inches='tight')
plt.savefig("{}/src_images/image_archive/{}_daily_user_acquisition".format(FILE_PATH, date.today().isoformat()), bbox_inches='tight')
