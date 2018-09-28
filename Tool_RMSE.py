import math

#RMSE for example
def RMSE(records):
    if len(records):
        return math.sqrt(sum([(rui-pui)*(rui-pui) for u,i,rui,pui in records]) / float(len(records)))
    else:
        return None
#MAE for example
def MAE(records):
    if len(records):
        return  sum([abs(rui-pui) for u,i,rui,pui in records]) / float(len(records))
    else:
        return None