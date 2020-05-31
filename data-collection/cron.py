from crontab import CronTab

cron = CronTab(user='root')  # Initialise a new CronTab instance
job = cron.new(command='python3 tfl_predictions.py')  # create a new task
# job.minute.on(0, 15, 30, 45)  # Define that it should be on every 0th, 15th, 30th and 45th minute
job.minute.on(53)
cron.write()  # 'Start' the task (i.e trigger the cron-job, but through the Python library instea