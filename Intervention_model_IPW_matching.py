import pandas as pd
import numpy as np
import datetime
import statsmodels.api as sm
import os
import sys
import logging
from Matcher import Matcher
import sklearn
from sklearn import metrics
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import xgboost as xgb
from sklearn.model_selection import GridSearchCV, KFold

## Setup code every *_task.py file needs
piper_path = os.path.abspath(__file__)
while os.path.basename(piper_path) != 'piper':
    piper_path = os.path.dirname(piper_path)
if piper_path not in sys.path:
    sys.path.append(piper_path)
from piper.piper_utils import setup_piper
setup_piper()

from piper.db.snowflake import SnowflakeDb
db = SnowflakeDb.singleton()

####required input - to be updated with yaml file###
feature = ['total_spend_fav_store_last_five_deliveries',
 'tot_visit_time',
 'n_platforms_visited_l28',
 'days_signup_to_activation',
 'min_days_between_reorder_l28',
 'avg_initial_charge_amt_usd_fav_store_last_five_deliveries',
 'total_items_fav_store_last_five_deliveries',
 'days_since_last_order_fav_store_last_five_deliveries',
 'min_days_between_reorder',
 'days_since_last_completed_order',
 'avg_change_tip_pct',
 'past_trial',
 'tip',
 'is_wao',
 'gtv_l28',
 'gtv_l91',
 'gtv_lifetime',
 'deliveries_l28',
 'deliveries_lifetime',
 'visits_l28',
 'visits_l91',
 'visits_lifetime',
 'signup_days',
 'activation_days',
 'activation_region_name']

reporting_metric = ['reporting_gtv_l28', 'reporting_is_mao']
variant = ['variant']

project_name = "project_name_"

tuning = False

base_table_ref = "sandbox_db.dianedou.matching_attributes_with_reporting_metrics"


## Function to write datafrom to DB
def create_result_table(df, tablename):
    ## reset singleton object for correct role
    logging.info("resetting singleton db object")
    db.start_time = None
    db._conn = None
    db.connection_settings['role']='IC_DS_ROLE'
    logging.info("after resetting")
    logging.info(db.__dict__)
    return db.df_to_sf(df=df,
                       tablename=tablename,
                       drop_if_exists=True,
                       database='INSTADATA',
                       schema='analysts',
                       role='public',
                       csv_encoding=None,
                       truncate=False)

def df_user_base_query(reference_date_pt, base_table_ref):
    query = "SELECT * FROM "+ base_table_ref
    df = SnowflakeDb.singleton().to_dataframe(query)
    df.columns = df.columns.str.lower()
    return df

## Basic data cleaning
def rename_cols(df):
    df.columns = np.char.replace(np.char.replace(np.char.lower(df.columns.values.astype('str')), ':overall::numeric(10,2)',''), 'fuga.','')
rename_cols(df)

# Categorical variables encoding
df_intervention = df.copy()
obj_df = df_intervention[feature].select_dtypes(include=['object']).copy()
X_coded = pd.get_dummies(df_intervention[feature], columns = obj_df.columns.to_list(), drop_first = True)
df_intervention_lite = pd.concat([df_intervention[reporting_metric + variant], X_coded], axis = 1)

##Propensity score generation
X_coded = X_coded.fillna(0)
X_coded = X_coded*1
T = df_intervention[variant]
X_train, X_test, t_train, t_test = sklearn.model_selection.train_test_split(X_coded, T, test_size=0.3, random_state=77)

#xgboost model tuning (optional)
def xgboost_tuning():

    params = {
        'n_estimators': [100, 500],
        'colsample_bytree': [0.75, 0.95],
        'subsample':[0.75, 0.95],

        'max_depth': [3,5],
        'learning_rate': [0.1, 0.3],

    #     'gamma': [0, 0.5, 1],
    #     'reg_alpha': [0, 0.5, 1],
    #     'reg_lambda': [0.5, 1, 5],

        'objective': ['multi:softprob'],
        'eval_metric': ['mlogloss'],
        'num_class':[2]
    }

    gs2 = GridSearchCV(xgb.XGBClassifier(n_jobs=-1), params, n_jobs=-1, cv=KFold(n_splits=3), scoring='roc_auc')
    gs2.fit(X_train, t_train)

    return (gs2.best_params_)

if tuning is True:
    params = xgboost_tuning()
else:
    params = {
    'n_estimators':500,
    'max_depth':3,
    'eta':.2,
    'gamma': 1,
    'reg_alpha': 0,
    'reg_lambda': 0.50,
    'colsample_bytree':.95,
    'subsample':.95,
    'objective': 'multi:softprob',
    'eval_metric': ['mlogloss'],
    'use_label_encoder':False,
    'num_class':2
    }

model = xgb.XGBClassifier(**params)
model.fit(
    X_train, t_train,
    eval_set = [(X_test, t_test)],
    early_stopping_rounds=5,
    verbose = True
    )

predicted_propensity = model.predict_proba(X_coded)[:,1]
df_intervention_lite['predicted_propensity'] = predicted_propensity

#ATT propensity scores
df_intervention_lite['propensity'] = np.where(df_intervention.variant == 1, 1, predicted_propensity / (1-predicted_propensity))

#ATT from Inverse Probability Weighting (IPW)
#ATT =  np.average(df_intervention.reporting_metric, weights = df_intervention_lite.propensity)


### ATT and confidence interval will be estimated by bootstrap in the following section
def make_bootstraps (data, n_bootstraps = 100):
    output_dc = {}
    unip = 0
    #get sample size
    b_size = data.shape[0]
    #get list of row indexes
    idx = [i for i in range(b_size)]
    # loop through number of bootstraps
    for b in range(n_bootstraps):
        #obtain bootstrap smaples with replacement
        sample_idx = np.random.choice(idx,replace=True,size=b_size)
#         b_samp = data.iloc[sample_idx, :]
        #compute number of unique values contained in the bootstrap sample
        unip  += len(set(sample_idx))
        #obtain out-of-bag samples for the current b
        oidx = list(set(idx) - set(sample_idx))
#         o_samp = np.array([])
#         if oidx:
#             o_samp = data.iloc[oidx,:]
        #store results
        output_dc['boot_'+str(b)] = {'boot':sample_idx,'test':oidx}
    return (output_dc)

def propensity_score_generation (data, feature, X_train, t_train, X_test, t_test, reporting_metric, b_iteration):
#     X_train, X_test, t_train, t_test = sklearn.model_selection.train_test_split(data, target_variable, test_size=0.3, random_state=77)
    att = {}
    model = xgb.XGBClassifier(**params)
    model.fit(
        X_train, t_train,
        eval_set = [(X_test, t_test)],
        early_stopping_rounds=5,
        verbose = True
            )
    predicted_propensity = model.predict_proba(data[feature])[:,1]
    data['predicted_propensity'] = predicted_propensity
    data['propensity'] = np.where(data.variant == 1, 1, predicted_propensity / (1-predicted_propensity))
    for i in range(len(reporting_metric)):
        metric = reporting_metric[i]
        reporting_result = pd.to_numeric(data[metric])
        if i == 0:
            att[b_iteration] = {metric : np.average(reporting_result, weights = data.propensity)}
        else:
            att[b_iteration].update({metric : np.average(reporting_result, weights = data.propensity)})
    return (att)

bootstrap_dc = make_bootstraps(X_coded, n_bootstraps = 5)
feature_coded = X_coded.columns.to_list()

bootstrap_att = {}

for b in bootstrap_dc:

    sample_idx = bootstrap_dc[b]['boot']
    test_idx = bootstrap_dc[b]['test']

    X_train = X_coded.iloc[sample_idx, :]
    t_train = T.iloc[sample_idx, :]
    X_test = X_coded.iloc[test_idx, :]
    t_test = T.iloc[test_idx, :]

    att_ite = propensity_score_generation(df_intervention_lite, feature_coded, X_train, t_train, X_test, t_test, reporting_metric, b_iteration = b)
    bootstrap_att.update(att_ite)

bootstrap_att_df = pd.DataFrame.from_dict(bootstrap_att, orient = 'index')

### Matching based on propensity score
df_intervention_matching = pd.concat([df_intervention[variant], X_coded], axis = 1)

def matching_cohort (data, variant_var = 'variant'):

    control = data[data[variant_var] == 0].reset_index(drop=True)
    test = data[data[variant_var] == 1].reset_index(drop=True)

    m = Matcher(test, control, yvar=variant_var, exclude=[])
    np.random.seed(20170925)
    m.fit_scores(balance=True, nmodels=10)
    m.predict_scores()
    m.match(method="random", nmatches=1, threshold=0.0005)
    matched_df = m.matched_data.reset_index(drop=True)

    return (matched_df)

df_matched = matching_cohort(df_intervention_matching)


### Outputs
create_result_table(df_intervention_lite , project_name + "propensity_score_df_" + datetime.datetime.now().isoformat())
create_result_table(bootstrap_att_df , project_name + "bootstrap_att_df_" + datetime.datetime.now().isoformat())
create_result_table(df_matched , project_name + "matched_df_" + datetime.datetime.now().isoformat())
