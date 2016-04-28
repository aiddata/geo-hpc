# utility for running parallel jobs with mpi4py
# mpi comms structured based on "09-task-pull" from:
#   https://github.com/jbornschein/mpi4py-examples


# from mpi4py import MPI

try:
    from mpi4py import MPI
    run_mpi = True

except:
    run_mpi = False

import types


def enum(*sequential, **named):
    """Generate an enum type object."""
    # source:
    # http://stackoverflow.com/questions/36932/how-can-i-represent-an-enum-in-python
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)


class NewParallel():
    """Contains basic structure for managing parallel processing tasks.

    Attributes:

        parallel (bool): x

        comm (str): x
        size (str): x
        rank (str): x
        status (str): x
        tags (str): x
        task_list (str): x

    """

    def __init__(self, parallel=True):

        if run_mpi == False:
            print "NewParallel warning: mpi4py could not be loaded"
            print "\tany instances of NewParallel will run in serial"
            self.parallel = False
        else:
            self.parallel = parallel


        if self.parallel:

            self.comm = MPI.COMM_WORLD
            self.size = self.comm.Get_size()
            self.rank = self.comm.Get_rank()

            self.status = MPI.Status()

            # define MPI message tags
            self.tags = enum('READY', 'DONE', 'EXIT', 'START', 'ERROR')

            if self.size == 1:
                self.parallel = False
                print "NewParallel warning: only one core found"
                print "\tany instances of NewParallel will run in serial"

        else:
            self.size = 1
            self.rank = 0


        self.task_list = None


    def set_task_list(self, input_list):
        """Set task list.

        Args:
            task_list (List[str]): x
        """
        if isinstance(input_list, list):
            self.task_list = input_list
        else:
            raise Exception("set_task_list: requires input of type list")


    def set_general_init(self, input_function):
        """Set general_init function.

        Args:
            task_list (List[str]): x
        """
        if hasattr(input_function, '__call__'):
            self.general_init = types.MethodType(input_function, self)
        else:
            raise Exception("set_general_init: requires input to be a function")


    def set_master_init(self, input_function):
        """Set master_init function.

        Args:
            task_list (List[str]): x
        """
        if hasattr(input_function, '__call__'):
            self.master_init = types.MethodType(input_function, self)
        else:
            raise Exception("set_master_init: requires input to be a function")


    def set_master_process(self, input_function):
        """Set master_process function.

        Args:
            task_list (List[str]): x
        """
        if hasattr(input_function, '__call__'):
            self.master_process = types.MethodType(input_function, self)
        else:
            raise Exception("set_master_process: requires input to be a function")


    def set_master_final(self, input_function):
        """Set master_final function.

        Args:
            task_list (List[str]): x
        """
        if hasattr(input_function, '__call__'):
            self.master_final = types.MethodType(input_function, self)
        else:
            raise Exception("set_master_final: requires input to be a function")


    def set_worker_job(self, input_function):
        """Set worker_job function.

        Args:
            task_list (List[str]): x
        """
        if hasattr(input_function, '__call__'):
            self.worker_job = types.MethodType(input_function, self)
        else:
            raise Exception("set_worker_job: requires input to be a function")


    def general_init(self):
        """Template only.

        Should be replaced by user created function using set_general_init method.

        Run by all processes (master and workers) at start
            of processing before any tasks are sent to workers.
        No args orreturn value.
        """
        pass


    def master_init(self):
        """Template only.

        Should be replaced by user created function using
            set_master_init method.

        Run by master only at start of processing before any tasks are
            sent to workers.
        No args or return value.
        """
        self.master_data = []


    def master_process(self, worker_data):
        """Template only.

        Should be replaced by user created function using
            set_master_process method

        Run by master only during processing for each task received
            from a worker.
        No return value.

        Args:
            value (str): x
        """
        self.master_data.append(worker_data)


    def master_final(self):
        """Template only.

        Should be replaced by user created function using
            set_master_final method

        Run by master only at end of processing after all tasks have
            been completed by workers.
        No args or return value.
        """
        master_data_stack = self.master_data


    def worker_job(self, task_id):
        """Template only.

        Should be replaced by user created function using
            set_worker_job method

        Run by work after receiving a task from master.

        Args:
            task_id (int): x

        Returns:
            results: object to be passed back from worker to master
        """
        task = self.task_list[task_id]

        results = task

        return results

    def run(self):
        """Run job in parallel or serial.
        """
        if self.parallel:
            self.run_parallel()
        else:
           self.run_serial()


    def run_serial(self):
        """Run job using set functions in serial."""
        if rank == 0:
            self.general_init()
            self.master_init()

            for i in range(len(self.task_list)):
                worker_result = self.worker_job(i)
                self.master_process(worker_result)

            self.master_final()


    def run_parallel(self):
        """Run job using set functions in parallel."""
        self.general_init()

        self.comm.Barrier()

        if self.rank == 0:

            # ==================================================
            # MASTER INIT

            self.master_init()

            # ==================================================

            task_index = 0
            num_workers = self.size - 1
            closed_workers = 0
            err_status = 0

            print("Master - starting with %d workers" % num_workers)

            # distribute work
            while closed_workers < num_workers:
                worker_data = self.comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=self.status)
                source = self.status.Get_source()
                tag = self.status.Get_tag()

                if tag == self.tags.READY:

                    if task_index < len(self.task_list):
                        print("Master - sending task %d to worker %d" % (task_index, source))

                        self.comm.send(task_index, dest=source, tag=self.tags.START)

                        task_index += 1

                    else:
                        self.comm.send(None, dest=source, tag=self.tags.EXIT)

                elif tag == self.tags.DONE:
                    print("Master - got data from worker %d" % source)

                    # ==================================================
                    # MASTER PROCESS

                    self.master_process(worker_data)

                    # ==================================================

                elif tag == self.tags.EXIT:
                    print("Master - worker %d exited. (%d)" % (source, num_workers))
                    closed_workers += 1

                elif tag == self.tags.ERROR:
                    print("Master - error reported by worker %d ." % source)
                    # broadcast error to all workers
                    for i in range(1, size):
                        self.comm.send(None, dest=i, tag=self.tags.ERROR)

                    err_status = 1
                    break


            if err_status == 0:
                print("Master - processing results")

                # ==================================================
                # MASTER FINAL

                self.master_final()

                # ==================================================

            else:
                print("Master - terminating due to worker error.")


        else:
            # Worker processes execute code below
            name = MPI.Get_processor_name()
            print("Worker - rank %d on %s." % (self.rank, name))
            while True:
                self.comm.send(None, dest=0, tag=self.tags.READY)
                task_id = self.comm.recv(source=0, tag=MPI.ANY_TAG, status=self.status)
                tag = self.status.Get_tag()

                if tag == self.tags.START:

                    # ==================================================
                    # WORKER JOB

                    worker_result = self.worker_job(task_id)

                    # ==================================================

                    # send worker_result back to master (master_process function)
                    self.comm.send(worker_result, dest=0, tag=self.tags.DONE)

                elif tag == self.tags.EXIT:
                    self.comm.send(None, dest=0, tag=self.tags.EXIT)
                    break

                elif tag == self.tags.ERROR:
                    print("Worker - error message from Master. Shutting down." % source)
                    # confirm error message received and exit
                    self.comm.send(None, dest=0, tag=self.tags.EXIT)
                    break


