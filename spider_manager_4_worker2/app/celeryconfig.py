# celeryconfig.py
broker_url = 'redis://localhost:6379/0'  # 替换为您实际的 Redis URL
result_backend = 'redis://localhost:6379/0'  # 替换为您实际的 Redis URL

# 关键设置
worker_prefetch_multiplier = 1  # worker 一次只预取一个任务
task_acks_late = True  # 只有在任务成功完成后才确认
worker_concurrency = 1  # 每个 worker 进程只使用一个工作线程
worker_max_tasks_per_child = 1  # 可选：每个子进程只处理一个任务后重启

# 开启事件监控
worker_send_task_events = True
task_send_sent_event = True

# 任务默认设置
task_default_queue = 'default'
task_queues = {
    # '1_queue': {'exchange': '1_queue', 'routing_key': '1_queue'},
    '2_queue': {'exchange': '2_queue', 'routing_key': '2_queue'}
}

# 允许任务在出错时重试
task_acks_on_failure_or_timeout = False

# 其他有用的设置
broker_connection_retry_on_startup = True