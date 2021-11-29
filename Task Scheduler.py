def left(i):
    return 2*i + 1
def right(i):
    return 2*i + 2

class MinHeapq:
    """ 
    A class that implements properties and methods that support a min priority queue data structure

    Attributes
    ----------
    heap : arr
        A Python list where key values in the min heap are stored
    heap_size: int
        An integer counter of the number of keys present in the max heap
    """  
    def mink(self):
        """
        Returns the task with the smallest priority value (closest urgency) in the priority queue. 
        
        Parameters
        ----------
        None

        Returns
        ----------
        int
            the task with the smallest priority value in the priority queue

        """
        return self.heap[0]
    
    def __init__(self):
        self.heap = []
        self.heap_size  = 0
        
    def parent(self, i):
        """
        Takes the index of the child node
        and returns the index of the parent node

        """
        return (i - 1)//2
    
    def heappush(self, task):
        """
        Insert a task into a priority queue 
        
        Parameters
        ----------
        key: int
            The task to be inserted

        Returns
        ----------
        None
        """
        self.heap.append(task)
        i = self.heap_size
        while i > 0 and self.heap[self.parent(i)] > self.heap[i]:
            j = self.parent(i)
            self.heap[i], self.heap[j] = self.heap[j], self.heap[i]
            i = j  
        self.heap_size+=1 
                 
    def heapify(self, i):
        """
        Creates a min heap from the index given
        
        Parameters
        ----------
        i: int
            The index of of the root node of the subtree to be heapify

        Returns
        ----------
        None
        """
        l = left(i)
        r = right(i)
        smallest = i
        if l < self.heap_size and self.heap[l] < self.heap[i]:
            smallest = l
        if r < self.heap_size and self.heap[r] < self.heap[smallest]:
            smallest = r
        if smallest != i:
            self.heap[i], self.heap[smallest] = self.heap[smallest], self.heap[i]
            self.heapify(smallest)

    def heappop(self, i=0):
        """
        returns the task with the smallest priority value in the min priority queue
        and removes it from the min priority queue
        
        Parameters
        ----------
        None

        Returns
        ----------
        int
            the task with closest urgency in the heap that is extracted
        """
        if self.heap_size < 1:
            raise ValueError('Heap underflow: There are no keys in the priority queue ')
        mink = self.heap[i]
        self.heap[i] = self.heap[-1]
        self.heap.pop()
        self.heap_size -= 1
        self.heapify(i)
        return mink

class Task:
    """
    - id: Task Id   
    - description: Short description of the task   
    - duration: Duration in minutes   
    - dependencies: An array of id's of tasks that the task depends on 
    - status: Current status of the task
    - multi_tasking: Tells whether the task can be multitasked or not
    - fixed: Bool for if the task has a fixed start time or can start at any time within its urgency
    - priority: The urgency of the task 
   
    """
    #Initializes an instance of Task
    def __init__(self,task_id,description,duration,dependencies, urgency=1440, multi_tasking=False, fixed=False, status="N"):
        self.id= task_id
        self.description=description
        self.duration=duration
        self.dependencies=dependencies
        self.status=status
        self.priority=urgency
        self.multi_tasking=multi_tasking
        self.fixed=fixed
        
    def __repr__(self):
        return f"{self.description} - id: {self.id}\n \tDuration:{self.duration}\n\tDepends on: {self.dependencies}\n\tMust be completed at most {self.priority} minutes after day start\n\tFixed task start: {self.fixed}\n\tStatus: {self.status}"

    def __lt__(self, other):
        """
        Defines what attributes of the Task class that we would compare when we perform logical operations on the tasks. In this case we compare their priorities.
        """
        return self.priority < other.priority
    
class TaskSchedulerWithMultiTasking:
    """
    A Daily Task Scheduler Using Priority Queues based on Urgency that allows for multi-tasking
    Attributes
    ----------
    tasks : arr
        A Python list of all the tasks we'd like to schedule
    priority_queue: arr
        The priority queue of tasks we are ready to execute
    fixed_tasks: arr
        The priority queue of tasks that must start at a fixed time
    """ 
    
    NOT_STARTED ='N'
    IN_PRIORITY_QUEUE = 'I'
    COMPLETED = 'C'
    
    def __init__(self, tasks):
        self.tasks = tasks
        self.priority_queue = MinHeapq()
        self.fixed_tasks = MinHeapq()
                
    def print_self(self):
        print('Input List of Tasks')
        for t in self.tasks:
            print(t)            
            
    def remove_dependency(self, task_id):
        """
        Input: list of tasks and task_id of the task just completed
        Output: lists of tasks with t_id removed
        """
        for t in self.tasks:
            if t.id != task_id and task_id in t.dependencies:
                t.dependencies.remove(task_id)           
            
    def get_tasks_ready(self):
        """ 
        Implements step 1 of the scheduler
        Input: list of tasks
        Output: list of tasks that are ready to execute (i.e. tasks with no pending task dependencies)
        """
        for task in self.tasks:
            if task.status == self.NOT_STARTED and not task.dependencies and not task.fixed: # If task has no dependencies and is not yet in queue and isn't a fixed task
                task.status = self.IN_PRIORITY_QUEUE # Change status of the task
                ## Push task into the priority queue
                self.priority_queue.heappush(task)
    
    def update_urgencies(self, time_elapsed=0):
        """
        Input: duration of the most recent executed task
        Output: boolean (checks the status of all tasks and returns True if at least one task has status = 'N'
        """
        for i in range(len(self.tasks)): 
            self.tasks[i].priority -= time_elapsed # Update all the task urgencies by subtracting the duration of the just executed task
        
    def check_unscheduled_tasks(self):
        """
        Input: list of tasks 
        Output: boolean (checks the status of all tasks and returns True if at least one task has status = 'N'
        """
        for task in self.tasks:
            if task.status == self.NOT_STARTED:
                return True
        return False   
    
    def get_fixed_tasks(self):
        """
        Get all tasks in the task list that have fixed start times
        """
        for i in self.tasks:
            if i.fixed:
                self.fixed_tasks.heappush(i)
    
    def format_time(self, time):
        return f"{time//60}h{time%60:02d}"
    
    # function that checks for and executes multi-tasking
    def multi_tasker(self, task, current_time):
        if (task.multi_tasking):
            temp = []
            while(self.priority_queue.heap_size > 0 and (not self.priority_queue.mink().multi_tasking or (self.fixed_tasks.heap_size > 0 and (self.priority_queue.mink().duration > self.fixed_tasks.mink().priority - self.fixed_tasks.mink().duration)))):
                # we continue to pop the most urgent task in the priority queue and store the task in a temporary array until the next task is multi-taskable its duration doesn't overshoot the fixed task start time
                temp.append(self.priority_queue.heappop())

            # if the priority queue has a value after the while loop, then we can multi-task with that task
            if self.priority_queue.heap:
                multitask = self.priority_queue.heappop()
                print(f"⏰Simple Scheduler at time {self.format_time(current_time)} started executing tasks {task.id} and {multitask.id} that take {task.duration} and {multitask.duration} mins respectively")
                
                # Increase current_time by the task that takes more time, because we will still be carrying out that task one we are done with the first
                if task.duration < multitask.duration:
                    current_time += task.duration
                    print(f"✅ Completed Task {task.id} - '{task.description}' at time {self.format_time(current_time)}\n")
                    current_time += multitask.duration - task.duration
                    print(f"✅ Completed Task {multitask.id} - '{multitask.description}' at time {self.format_time(current_time)}\n")
                    self.update_urgencies(multitask.duration)
                else:
                    current_time += multitask.duration
                    print(f"✅ Completed Task {multitask.id} - '{multitask.description}' at time {self.format_time(current_time)}\n")
                    current_time += task.duration - multitask.duration
                    print(f"✅ Completed Task {task.id} - '{task.description}' at time {self.format_time(current_time)}\n")
                    self.update_urgencies(task.duration)
                
                # Remove dependencies and update task status. 
                self.remove_dependency(task.id)
                self.remove_dependency(multitask.id)
                task.status = self.COMPLETED
                multitask.status = self.COMPLETED

            # if the priority queue is empty after the while loop, then none of the tasks can be multitasked within the interval between the current time and the start of the fixed task
            # so we just perform the initial task as usual
            else:
                print(f"⏰Simple Scheduler at time {self.format_time(current_time)} started executing task {task.id} that takes {task.duration} mins")
                current_time += task.duration            
                print(f"✅ Completed Task {task.id} - '{task.description}' at time {self.format_time(current_time)}\n") 
                # if the task is completed, it cannot be a dependency on other tasks, so remove it from the dependency list 
                self.update_urgencies(task.duration)
                self.remove_dependency(task.id)
                task.status = self.COMPLETED

            # once we've carried out the task, we restore all the other tasks to the priority queue in the order we popped them 
            for i in temp:
                self.priority_queue.heappush(i)
            
        # if the task isn't multi-taskable, we do not need to perform any other checks, so we perform the task as regular
        else:   
            print(f"⏰Simple Scheduler at time {self.format_time(current_time)} started executing task {task.id} that takes {task.duration} mins")
            current_time += task.duration            
            print(f"✅ Completed Task {task.id} - '{task.description}' at time {self.format_time(current_time)}\n") 
            # if the task is completed, it cannot be a dependency on other tasks, so remove it from the dependency list 
            self.update_urgencies(task.duration)
            self.remove_dependency(task.id)
            task.status = self.COMPLETED
            
        # return the current time after performing these operations
        return current_time
    
    def run_task_scheduler(self, starting_time = 480):
        current_time = starting_time
        self.get_fixed_tasks(); # move all the fixed tasks to the fixed_tasks priority queue
        while self.check_unscheduled_tasks() or self.priority_queue.heap:
            # Extract tasks ready to execute (non-fixed tasks without dependencies) and push them into the priority queue
            self.get_tasks_ready()
            if self.priority_queue.heap_size > 0 :  # Check for tasks in the priority queue.      
                if self.fixed_tasks.heap_size > 0 and self.priority_queue.mink().duration > self.fixed_tasks.mink().priority - self.fixed_tasks.mink().duration: # run this 'if' loop if the time it would take to complete the current task overshoots the start time of the next fixed task
                    temp = []
                    while(self.priority_queue.heap_size > 0 and self.priority_queue.mink().duration > self.fixed_tasks.mink().priority - self.fixed_tasks.mink().duration):
                        # we continue to pop the most urgent task in the priority queue and store the task in a temporary array until its duration doesn't overshoot the fixed task start time
                        temp.append(self.priority_queue.heappop())
                        
                    # if the priority queue has a value after the while loop, then we can perform that task within the interval.
                    if self.priority_queue.heap:
                        task = self.priority_queue.heappop()  
                        
                        # call the multi_tasker function for the task and update the current time to the time once the multi-tasker is done
                        current_time = self.multi_tasker(task, current_time)
                        
                    # if the priority queue is empty after the while loop, then none of the tasks can be done within the interval between the current time and the start of the fixed task
                    # so we just wait until the fixed task is set to start
                    else:
                        # update the current time and the urgencies by the amount of time we skipped to the fixed task start time
                        current_time = current_time + self.fixed_tasks.mink().priority - self.fixed_tasks.mink().duration
                        self.update_urgencies(self.fixed_tasks.mink().priority - self.fixed_tasks.mink().duration)
                        task = self.fixed_tasks.heappop()
                        
                        # call the multi_tasker function for the task and update the current time to the time once the multi-tasker is done
                        current_time = self.multi_tasker(task, current_time)
                        
                    # once we've carried out the task, we restore all the other tasks to the priority queue in the order we popped them 
                    for i in temp:
                        self.priority_queue.heappush(i)
                        
                # if the time it would take to complete the current task doesn't overshoot the start time of the next fixed task, we perform the task as normal
                else:
                    task = self.priority_queue.heappop()   
                    
                    # call the multi_tasker function for the task and update the current time to the time once the multi-tasker is done
                    current_time = self.multi_tasker(task, current_time)
                    
            # if the priority queue is empty, we can proceed to perform the remaining fixed tasks
            else:
                while self.fixed_tasks.heap_size > 0:
                    # update the current time and the urgencies by the amount of time we skipped to the fixed task start time
                    current_time = current_time + self.fixed_tasks.mink().priority - self.fixed_tasks.mink().duration
                    self.update_urgencies(self.fixed_tasks.mink().priority - self.fixed_tasks.mink().duration)
                    task = self.fixed_tasks.heappop()
                    
                    # call the multi_tasker function for the task and update the current time to the time once the multi-tasker is done
                    current_time = self.multi_tasker(task, current_time)
                
        # return the total time we took to perform all the tasks
        total_time = current_time - starting_time             
        print(f"🏁 Completed all planned tasks in {total_time//60}h{total_time%60:02d}min")