## The Background

Develpop 'The Background' Section the flow will be : 

- The history of database and their evolution (please cite from paper) until we arrive on zeta-bytes era.
- Zetta-bytes era (cite from paper), the growth of internet make database have very crucial role
- Based on that need and position of database, database expected not to just store and read data but extend their capabilities
- Also database need to extend on the platform that they install. So many variaties of challenge that database face
- So usually database will be categorized based on their main purpose despite the fact that maybe some existing database pose
themself as general purpose
- From usage prespective it might have 3 category (please cite from paper) :
    - OLTP that will focus handling transactional process, have efficient transaction mechanism
    - OLAP that will focus handling analytical processs, have efficient data retrieval mechanism
    - HTAP that focus on both side analytical and transactional, usually poses as general purpose
- From how it install on platform and the interaction (please cite from paper) :
    - Client - Server model, A long running process that installed on the platform, require a network access because
    it interact with client over a network
    - Embedded, no network access required, operate database directly from function call so it might have shared space with
    the client that call / operate the database
- This study starting with an interested paper "SQLite: Past, Present, and Future"
- That paper talking about SQLite, their capabilites, etc. 
- Interesting part begin when mention about duck db, a new emebedded database that poses analytic embedded database
- Paper also show a comparison benchmark of duck db and sqlite, while sqlite have great performance on TATP benchmark (OLTP)
- But, duck db have poor performance, vice cersa, duck db have a great performance on SSB benchmark (OLAP) but sqlite not
- So, that's is like flipping coin, so this study aim to propose a new solution that will sitting between and general purpose 
wannabe for an embedded type database, it's called "Bambang DB A Rudimentary Embedded Database"
- Bambang DB will start from OLTP side then it extend and improve the performance of OLAP
- Bambang DB recipes behind that is some improvement on the storage engine and data retrieval flow
- Bambang will introduce the used of some new algorithm such as Partitioned B+ Tree for storage engine 
and introducing parallel scan capabilities that employ multi-thread (that sqlite according to prev paper dont have that one)
- Using that advantage on introduced imrpovement Bambang DB is projected can have good performance both for OLTP and OLAP type workload (HTAP)

## Problem Statement

Develop the 'Problem Statement' section, the flow will be like this : 


## Scope of the Study

## Objectives of the study

## Benefits of the study