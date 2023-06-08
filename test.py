import dash
from matplotlib import pyplot as plt
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
from dash import dcc
from dash import html


# MongoDB 클라이언트 인스턴스 생성
client = MongoClient(
                     host = "localhost",
                     port = 27017,
                     username = "root",
                     password = "root",
                    )

# 테스트 DB 정의
db = client['project2']
news = db['news']


#1. Published : 시간대 별로 뉴스
cursor1 = news.aggregate([{
                          "$project": {
                              "published_time": {
                          "$hour": {
                          "$dateFromString": {
                              "dateString": "$published",
                          },
                          },
                          },
                          },
                          },
                          {
                          "$group": {
                              "_id": "$published_time",
                              "numberof": {
                          "$sum": 1
                          },
                          },
                          },
                          {
                          "$sort": {
                              "_id": 1
                          },
                          },])

list_cursor1 = list(cursor1)
df1=pd.DataFrame(list_cursor1)
df1.rename(columns={"_id":"time"}, inplace=True)

fig1 = px.line(df1,x="time",y="numberof", title='시간 대 별 뉴스 개수')


# 2. Domain rank : 도메인 랭크 별로 기사 몇개 출판했는지
cursor2 = news.aggregate([
                           { "$group": {
                               "_id": {
                                 "site": "$thread.site",
                                 "domain_rank": "$thread.domain_rank",
                               }, "numberof": {
                                 "$sum": 1,
                               },
                             },
                           }, {
                             "$project": {
                               "site_domain": "$_id",
                               "numberof": 1,
                               "_id": 0,
                             },
                           }, { "$sort": {
                               "numberof": -1 } }
                         ])

list_cursor2 = list(cursor2)
df2 = pd.DataFrame(list_cursor2)

tmp1 = list(df2["site_domain"])
list_domain = list()
for i in tmp1:
    try :
        sentence = i["site"] + '(' +  str(i["domain_rank"]) + ')'
    except:
        sentence = i["site"]
    list_domain.append(sentence)

df_domain = pd.DataFrame(list_domain)
domain=['reuters.com(408)', 'cnbc.com(767)', 'wsj.com(387)', 'fortune.com(1196)']
number=[197514, 85197, 17794, 5737]
fig2 = px.bar(x=df_domain[0], y=df2["numberof"], title='도메인 랭크 별 뉴스 개수')

# 3. Thread.social : 어떤 SNS에서 가장 공유가 많이 되었는지
cursor3 = news.aggregate([
                           { "$group": {
                               "_id": "null",
                               "gplus": {
                                 "$sum": "$thread.social.gplus.shares",
                               }, "pinterest": {
                                 "$sum": "$thread.social.pinterest.shares",
                               }, "vk": {
                                 "$sum": "$thread.social.vk.shares",
                               }, "linkedin": {
                                 "$sum": "$thread.social.linkedin.shares",
                               }, "facebook": {
                                 "$sum": "$thread.social.facebook.shares",
                               }, "stumbledupon": {
                                 "$sum": "$thread.social.stumbledupon.shares",
                               } } }
                         ])

list_cursor3 = list(cursor3)
df3 = pd.DataFrame(list_cursor3)

list_sns = list()
list_number = list()
idx = 0
for i in df3.columns:
    if i != "_id":
        list_sns.append(i)
        list_number.append(df3.loc[0][i])
    idx = idx + 1

pd_sns = pd.DataFrame(list_sns)
pd_number = pd.DataFrame(list_number)

fig3 = px.bar(x=pd_sns[0], y=pd_number[0], title='SNS 공유 현황')
fig3.update_yaxes(type="log")

# 4번 내용 시작
pipeline = [
             { "$unwind": "$entities" },
             { "$unwind": "$entities.persons"},
             { "$unwind": "$entities.persons.sentiment"},
             { "$match" : { "entities.persons.sentiment" :
           						 {"$in" : ["negative", "neutral", "positive"]}}},
             { "$project": {
               "published_time": {"$substr": ["$published", 11, 2]},
               "entities": 1
             }},
             { "$group": {
                 "_id": "$published_time",
                     "negativeCount": {
                   "$sum": {
                     "$cond": [{ "$eq": ["$entities.persons.sentiment", "negative"] }, 1, 0]
                   }
                 },
                 "neutralCount": {
                   "$sum": {
                     "$cond": [{ "$eq": ["$entities.persons.sentiment", "neutral"] }, 1, 0]
                   }
                 },
                 "positiveCount": {
                   "$sum": {
                     "$cond": [{ "$eq": ["$entities.persons.sentiment", "positive"] }, 1, 0]
                   }
                 }
             }},
             { "$sort": { "_id" : 1} }
           ]
results4 = news.aggregate(pipeline)
df4 = pd.DataFrame(results4)


n_df4_1 = pd.DataFrame(abs(df4['negativeCount'] - df4['negativeCount'].mean())/(df4['negativeCount'].std()))
n_df4_2 = pd.DataFrame(abs(df4['neutralCount'] - df4['neutralCount'].mean())/(df4['neutralCount'].std()))
n_df4_3 = pd.DataFrame(abs(df4['positiveCount'] - df4['positiveCount'].mean())/(df4['positiveCount'].std()))

fig4_1 = go.Figure()
fig4_1.add_trace(go.Scatter(x=df4["_id"], y=df4["negativeCount"],
                    mode='lines',
                    name='negativeCount'))
fig4_1.add_trace(go.Scatter(x=df4["_id"], y=df4["neutralCount"],
                    mode='lines',
                    name='neutralCount'))
fig4_1.add_trace(go.Scatter(x=df4["_id"], y=df4["positiveCount"],
                    mode='lines',
                    name='positiveCount'))

fig4_1.update_layout(title_text="4. 시간대 별로 사람들의 감정 흐름 시각 정규화 전")


fig4_2 = go.Figure()
fig4_2.add_trace(go.Scatter(x=df4["_id"], y=n_df4_1["negativeCount"],
                    mode='lines',
                    name='negativeCount'))
fig4_2.add_trace(go.Scatter(x=df4["_id"], y=n_df4_2["neutralCount"],
                    mode='lines',
                    name='neutralCount'))
fig4_2.add_trace(go.Scatter(x=df4["_id"], y=n_df4_3["positiveCount"],
                    mode='lines',
                    name='positiveCount'))
fig4_2.update_layout(title_text="4. 시간대 별로 사람들의 감정 흐름 시각 정규화 후")

# 5번 쿼리 시작
cursor5 = news.aggregate([
                           { "$unwind": "$entities" },
                           { "$unwind": "$entities.organizations"},
                           { "$unwind": "$entities.organizations.sentiment"},
                           { "$match" : { "entities.organizations.sentiment" :
                         		{"$in" : ["negative", "neutral", "positive"]}}},
                           { "$group": {
                               "_id": "$thread.site",
                                   "negativeCount": {
                                 "$sum": {
                                   "$cond":
                         						[{ "$eq": ["$entities.organizations.sentiment", "negative"] }, 1, 0]
                                 }
                               },
                               "neutralCount": {
                                 "$sum": {
                                   "$cond":
                         						[{ "$eq": ["$entities.organizations.sentiment", "neutral"] }, 1, 0]
                                 }
                               },
                               "positiveCount": {
                                 "$sum": {
                                   "$cond":
                         						[{ "$eq": ["$entities.organizations.sentiment", "positive"] }, 1, 0]
                                 }
                               }
                           }},
                           { "$sort": { "_id" : 1} }
                         ])
list_cur5 = list(cursor5)
df5 = pd.DataFrame(list_cur5)
n_df5_1 = pd.DataFrame(abs(df5['negativeCount'] - df5['negativeCount'].mean())/(df5['negativeCount'].std()))
n_df5_2 = pd.DataFrame(abs(df5['neutralCount'] - df5['neutralCount'].mean())/(df5['neutralCount'].std()))
n_df5_3 = pd.DataFrame(abs(df5['positiveCount'] - df5['positiveCount'].mean())/(df5['positiveCount'].std()))

fig5 = go.Figure()
fig5.add_trace(go.Bar(x=df5["_id"], y=df5["negativeCount"],
                    name='negativeCount'))
fig5.add_trace(go.Bar(x=df5["_id"], y=df5["neutralCount"],
                    name='neutralCount'))
fig5.add_trace(go.Bar(x=df5["_id"], y=df5["positiveCount"],
                    name='positiveCount'))
fig5.update_layout(title_text="5. 사이트별로 부정, 중립, 긍정의 비율 정규화 전")

fig5_2 = go.Figure()
fig5_2.add_trace(go.Bar(x=df5["_id"], y=n_df5_1["negativeCount"],
                    name='negativeCount'))
fig5_2.add_trace(go.Bar(x=df5["_id"], y=n_df5_2["neutralCount"],
                    name='neutralCount'))
fig5_2.add_trace(go.Bar(x=df5["_id"], y=n_df5_3["positiveCount"],
                    name='positiveCount'))
fig5_2.update_layout(title_text="5. 사이트별로 부정, 중립, 긍정의 비율 정규화 후")

# 6번 쿼리 시작
cursor6 = news.aggregate([
                           { "$group": {
                               "_id": "$thread.site",
                               "spam_avg": {"$avg": "$thread.spam_score"}
                           }},
                           { "$project": {"_id": 0, "site_name": "$_id", "spam_avg": 1}},
                           { "$sort": { "spam_avg" : -1} }
                         ])
list_cursor6 = list(cursor6)
df6 = pd.DataFrame(list_cursor6)
fig6 = px.bar(df6, x='site_name', y='spam_avg')
fig6.update_layout(title_text="6. 스팸 스코어가 많이 있는 사이트")


# 7번 쿼리
cursor7 = news.aggregate([
    {
        '$project': {
            'published_time': {
                '$hour': {
                    '$dateFromString': {
                        'dateString': '$published'
                    }
                }
            },
            'length': {
                '$strLenCP': '$text'
            }
        }
    }, {
        '$group': {
            '_id': '$published_time',
            'avg_text_length': {
                '$avg': '$length'
            }
        }
    }, {
        '$sort': {
            '_id': 1
        }
    }
])
list_cur7 = list(cursor7)

df7 = pd.DataFrame(list_cur7)
df7.rename(columns={'_id': 'hour'}, inplace=True)
fig7 = px.bar(df7, x="hour", y="avg_text_length",
              title="7. Average 'text' length by 'hour'")

# 8번 쿼리 시작
cursor8 = news.aggregate([
    {
        '$project': {
            'title_length': {
                '$strLenCP': '$thread.title'
            },
            'likes': '$thread.social.facebook.likes'
        }
    }, {
        '$bucket': {
            'groupBy': '$title_length',
            'boundaries': [
                0, 50, 100, 150, 200, 250, 300
            ],
            'output': {
                'likes': {
                    '$avg': '$likes'
                }
            }
        }
    }
])
list_cur8 = list(cursor8)

df8 = pd.DataFrame(list_cur8)
df8.rename(columns={'_id': 'title_length'}, inplace=True)
fig8 = px.bar(df8, x="title_length", y="likes",
              title="8. Average number of 'likes' by 'title' length")

# 9번 쿼리 시작
cursor9 = news.aggregate([
        {
            '$project': {
                'site': '$thread.site',
                'sections': '$thread.site_section'
            }
        }, {
            '$group': {
                '_id': '$site',
                'type_of_sections': {
                    '$addToSet': '$sections'
                }
            }
        }, {
            '$unwind': {
                'path': '$type_of_sections'
            }
        }, {
            '$group': {
                '_id': '$_id',
                'how_many_sections': {
                    '$sum': 1
                }
            }
        }
    ])
list_cur9 = list(cursor9)

df9 = pd.DataFrame(list_cur9)
df9.rename(columns={'_id': 'site'}, inplace=True)
fig9 = px.bar(df9, x="site", y="how_many_sections",
              title="9. Number of 'sections' for each 'site(a news agency)'")

# 인터페이스
app = dash.Dash()
app.layout = html.Div([
    html.Div([
        html.H1("금융 뉴스 데이터 분석", style={"display":"flex", "justify-content": "center", "text-align": "center", "padding": "20px 0 20px 0", "background-color": "#F6EDE1"}),
        html.H2("메뉴바"),
        html.A("1. 시간대 별 출판된 기사의 수", href="#1"),
        html.A("2. 'domain rank'와 출판 기사 개수의 연관성", href="#2"),
        html.A("3. 어떤 SNS에서 가장 공유가 많이 되는가", href="#3"),
        html.A("4. 시간대 별로 사람들의 감정 흐름 시각", href="#4"),
        html.A("5. 사이트별로 부정, 중립, 긍정의 비율", href="#5"),
        html.A("6. 스팸 스코어가 많이 있는 사이트", href="#6"),
        html.A("7. 시간대 별 본문 내용의 길이 평균값", href="#7"),
        html.A("8. 제목의 길이와 'facebook.likes'의 관계", href="#8"),
        html.A("9. 각 언론사가 다루는 'section' 가짓수", href="#9"),
    ], style={"display": "flex", "flex-direction": "column", }),

    html.Div([
        html.H3('1. 시간대 별 출판된 기사의 수', id="1", style={"display":"flex", "justify-content": "center", "text-align": "center", "padding-top": "20px"}),
        dcc.Graph(figure=fig1),
    ], style={"border": "1px solid gray", "margin":"65px 0 0 0"}),

    html.Div([
        html.H3("2. 'domain rank'와 출판 기사 개수의 연관성", id="2", style={"display":"flex", "justify-content": "center", "text-align": "center", "padding-top": "20px"}),
        dcc.Graph(figure=fig2),
    ], style={"border": "1px solid gray", "margin":"65px 0 0 0"}),


    html.Div([
        html.H3("3. 어떤 SNS에서 가장 공유가 많이 되는가", id="3", style={"display":"flex", "justify-content": "center", "text-align": "center", "padding-top": "20px"}),
        dcc.Graph(figure=fig3),
    ], style={"border": "1px solid gray", "margin":"65px 0 0 0"}),


    html.Div([
        html.H3('4. 시간대 별로 사람들의 감정 흐름 시각', id="4", style={"display":"flex", "justify-content": "center", "text-align": "center", "padding-top": "20px"}),
        dcc.Graph(figure=fig4_1),
        dcc.Graph(figure=fig4_2),
    ], style={"border": "1px solid gray", "margin":"65px 0 0 0"}),


    html.Div([
        html.H3('5. 사이트별로 부정, 중립, 긍정의 비율', id="5", style={"display":"flex", "justify-content": "center", "text-align": "center", "padding-top": "20px"}),
        dcc.Graph(figure=fig5),
        dcc.Graph(figure=fig5_2),
    ], style={"border": "1px solid gray", "margin":"65px 0 0 0"}),



    html.Div([
        html.H3('6. 스팸 스코어가 많이 있는 사이트', id="6", style={"display":"flex", "justify-content": "center", "text-align": "center", "padding-top": "20px"}),
        dcc.Graph(figure=fig6),
    ], style={"border": "1px solid gray", "margin":"65px 0 0 0"}),


    html.Div([
        html.H3('7. 시간대 별 본문 내용의 길이 평균값', id="7", style={"display":"flex", "justify-content": "center", "text-align": "center", "padding-top": "20px"}),
        dcc.Graph(figure=fig7),
    ], style={"border": "1px solid gray", "margin":"65px 0 0 0"}),


    html.Div([
        html.H3("8. 제목의 길이와 'facebook.likes'의 관계", id="8", style={"display":"flex", "justify-content": "center", "text-align": "center", "padding-top": "20px"}),
        dcc.Graph(figure=fig8),
    ], style={"border": "1px solid gray", "margin":"65px 0 0 0"}),


    html.Div([
        html.H3("9. 각 언론사가 다루는 'section' 가짓수", id="9", style={"display":"flex", "justify-content": "center", "text-align": "center", "padding-top": "20px"}),
        dcc.Graph(figure=fig9),
    ], style={"border": "1px solid gray", "margin":"65px 0 0 0"}),

], style={"padding": "0 40px 0 40px", "margin-bottom": "150px"})

app.run_server(debug=False, use_reloader=True)  # Turn off reloader if inside Jupyter
