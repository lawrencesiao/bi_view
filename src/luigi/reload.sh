#!/bin/bash

PYENV=bi
APPDIR=$(dirname $0)

TODAY=$(date "+%Y-%m-%d")
YESTERDAY=$(date --date='yesterday' "+%Y-%m-%d")

cd $APPDIR
source $HOME/.virtualenvs/$PYENV/bin/activate

#PYTHONPATH='' luigi --module action_tasks MonthlyAction --interval 2015-11
#PYTHONPATH='' luigi --module action_tasks MonthlyAction --interval 2015-12
#PYTHONPATH='' luigi --module action_tasks MonthlyAction --interval 2016-01
#PYTHONPATH='' luigi --module action_tasks MonthlyAction --interval 2016-02
#PYTHONPATH='' luigi --module action_tasks MonthlyAction --interval 2016-03
#PYTHONPATH='' luigi --module action_tasks MonthlyAction --interval 2016-04
#PYTHONPATH='' luigi --module action_tasks MonthlyAction --interval 2016-05
#PYTHONPATH='' luigi --module action_tasks MonthlyAction --interval 2016-06


PYTHONPATH='' luigi --module sales_tasks MonthlySales --interval 2016-02
PYTHONPATH='' luigi --module sales_tasks MonthlySales --interval 2016-03
PYTHONPATH='' luigi --module sales_tasks MonthlySales --interval 2016-04
PYTHONPATH='' luigi --module sales_tasks MonthlySales --interval 2016-05
PYTHONPATH='' luigi --module sales_tasks MonthlySales --interval 2016-06
PYTHONPATH='' luigi --module sales_tasks MonthlySales --interval 2016-07
PYTHONPATH='' luigi --module sales_tasks MonthlySales --interval 2016-08
PYTHONPATH='' luigi --module sales_tasks MonthlySales --interval 2016-09
PYTHONPATH='' luigi --module sales_tasks MonthlySales --interval 2016-10
PYTHONPATH='' luigi --module sales_tasks MonthlySales --interval 2016-11
PYTHONPATH='' luigi --module sales_tasks MonthlySales --interval 2016-12


#PYTHONPATH='' luigi --module register_tasks MonthlyRegister --interval 2015-11
#PYTHONPATH='' luigi --module register_tasks MonthlyRegister --interval 2015-12
#PYTHONPATH='' luigi --module register_tasks MonthlyRegister --interval 2016-01
#PYTHONPATH='' luigi --module register_tasks MonthlyRegister --interval 2016-02
#PYTHONPATH='' luigi --module register_tasks MonthlyRegister --interval 2016-03
#PYTHONPATH='' luigi --module register_tasks MonthlyRegister --interval 2016-04
#PYTHONPATH='' luigi --module register_tasks MonthlyRegister --interval 2016-05
#PYTHONPATH='' luigi --module register_tasks MonthlyRegister --interval 2016-06


PYTHONPATH='' luigi --module sales_tasks MonthlySalesByItem --interval 2016-03
PYTHONPATH='' luigi --module sales_tasks MonthlySalesByItem --interval 2016-04
PYTHONPATH='' luigi --module sales_tasks MonthlySalesByItem --interval 2016-05
PYTHONPATH='' luigi --module sales_tasks MonthlySalesByItem --interval 2016-06
PYTHONPATH='' luigi --module sales_tasks MonthlySalesByItem --interval 2016-07
PYTHONPATH='' luigi --module sales_tasks MonthlySalesByItem --interval 2016-08
PYTHONPATH='' luigi --module sales_tasks MonthlySalesByItem --interval 2016-09
PYTHONPATH='' luigi --module sales_tasks MonthlySalesByItem --interval 2016-10
PYTHONPATH='' luigi --module sales_tasks MonthlySalesByItem --interval 2016-11
PYTHONPATH='' luigi --module sales_tasks MonthlySalesByItem --interval 2016-12

#PYTHONPATH='' luigi --module cancel_tasks MonthlyCancel --interval 2016-03
#PYTHONPATH='' luigi --module cancel_tasks MonthlyCancel --interval 2016-04
#PYTHONPATH='' luigi --module cancel_tasks MonthlyCancel --interval 2016-05
#PYTHONPATH='' luigi --module cancel_tasks MonthlyCancel --interval 2016-06


#PYTHONPATH='' luigi --module items_tasks MonthlyItems --interval 2016-03
#PYTHONPATH='' luigi --module items_tasks MonthlyItems --interval 2016-04
#PYTHONPATH='' luigi --module items_tasks MonthlyItems --interval 2016-05
#PYTHONPATH='' luigi --module items_tasks MonthlyItems --interval 2016-06


for i in 2016-12-01 2016-12-02 2016-12-03 2016-12-04 2016-12-05 2016-12-06 2016-12-07 2016-12-08 2016-12-09 2016-12-10 2016-12-11 2016-12-12 2016-12-13 2016-12-14 2016-12-15 
do
#   PYTHONPATH='' luigi --module action_tasks DailyAction --date=$i
   PYTHONPATH='' luigi --module sales_tasks DailySales --date=$i
#   PYTHONPATH='' luigi --module cancel_tasks DailyCancel --date=$i
#   PYTHONPATH='' luigi --module items_tasks DailyItems --date=$i
 #  PYTHONPATH='' luigi --module register_tasks DailyRegister --date=$i
done


for i in 2017-01-01 2017-01-02 2017-01-03 2017-01-04
do
#   PYTHONPATH='' luigi --module action_tasks DailyAction --date=$i
   PYTHONPATH='' luigi --module sales_tasks DailySales --date=$i
#   PYTHONPATH='' luigi --module cancel_tasks DailyCancel --date=$i
#   PYTHONPATH='' luigi --module items_tasks DailyItems --date=$i
 #  PYTHONPATH='' luigi --module register_tasks DailyRegister --date=$i
done

for i in {1..52}
do
	PYTHONPATH='' luigi --module item_tasks WeeklyItem --interval 2016-W$i
	PYTHONPATH='' luigi --module sales_tasks WeeklySales --interval 2016-W$i
	PYTHONPATH='' luigi --module sales_tasks WeeklySalesByItem --interval 2016-W$i
	
done

