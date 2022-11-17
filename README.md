**Incrementality analysis toolkit**

Diane Dou 2022-11-07

This folder contains causal inference methods used for analyzing incrementality of Instacart initiatives. 

**Main objective of the analyses**

Often times, we resort to observational studies when A/B test is not applicatble or not sufficient in measuring initiatives' impact. The automated analysis aims to estimate (heterogeneous) treatment effects with categorical treatments and implemented forest-based method to address high-dimensional data with non-parametric solutions.

**Models included in the tookkit**

- Doubly robust eatimation 
  - Causal forest (General random forest, grf)

- Inverse propensity score estimation 
  - XGBoost Classifier 


**Main model output**

- DR estimator for average (heterogeneous) treatment effect
- Inverse propensity score re-weighting estimator for average treatment effect 
  - Inverse propensity score 
  - Synthetic control groups (control groups matched with exposed users using the propensity score)

See more detailed documentaion here: https://docs.google.com/document/d/1jy2zDpmAMUFwDY5Z78ncIYW_fQfEE8Wt4NF6DpMzXKI/edit#. 


Reference 
- Athey, S., & Imbens, G. (n.d.). Recursive partitioning for heterogeneous causal effects | PNAS. Retrieved November 15, 2022, from https://www.pnas.org/doi/10.1073/pnas.1510489113 
- Funk MJ, Westreich D, Wiesen C, Stürmer T, Brookhart MA, Davidian M. Doubly robust estimation of causal effects. Am J Epidemiol. 2011 Apr 1;173(7):761-7. doi: 10.1093/aje/kwq439. Epub 2011 Mar 8. PMID: 21385832; PMCID: PMC3070495
- Austin, Peter & Mamdani, Muhammad. (2006). A comparison of propensity score methods: A case-study estimating the effectiveness of post-AMI statin use. Statistics in medicine. 25. 2084-106. 10.1002/sim.2328. 
