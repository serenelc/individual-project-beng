from crontab import CronTab

cron = CronTab(user='root')  # Initialise a new CronTab instance
job = cron.new(command='python3 tfl_predictions.py')  # create a new task
job.minute.on(0, 15, 30, 45)  # Define that it should be on every 15th minute from the hour

cron.write()  # trigger the cron job