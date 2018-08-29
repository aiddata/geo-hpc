"""
utility for running parallel jobs with mpi4py
mpi comms structured based on "09-task-pull" from:
    https://github.com/jbornschein/mpi4py-examples
"""

# from mpi4py import MPI

try:
    from mpi4py import MPI
    run_mpi = True

except:
    run_mpi = False

import types
import traceback

import time
import json
from copy import deepcopy
from warnings import warn

def enum(*sequential, **named):
    """Generate an enum type object."""
    # source:
    # http://stackoverflow.com/questions/36932/how-can-i-represent-an-enum-in-python
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)


from bson import ObjectId

# https://stackoverflow.com/a/16586277
class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        return json.JSONEncoder.default(self, o)



# capture function stdout for printing output during
# parallel jobs in uninterupted blocks
# source: http://stackoverflow.com/a/16571630
from cStringIO import StringIO
import sys

class Capturing(list):
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self
    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        sys.stdout = self._stdout


class NewParallel(object):
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

    def __init__(self, parallel=True, capture=False, print_worker_log=True):

        if run_mpi == False:
            print "NewParallel warning: mpi4py could not be loaded"
            print "\tany instances of NewParallel will run in serial"
            self.parallel = False
        else:
            self.parallel = parallel

        self.capture = capture
        self.print_worker_log=print_worker_log

        if self.parallel:

            self.processor_name = MPI.Get_processor_name()

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


        self.task_count = 0
        self.task_list = None

        self.use_master_update = False
        self.update_interval = None


    def set_task_list(self, input_list):
        """Set task list.

        Args:
            input_list (list): x
        """
        if isinstance(input_list, list):
            self.task_list = input_list
            self.set_task_count(len(self.task_list))
        else:
            raise Exception("set_task_list: requires input of type list")


    def set_task_count(self, count):
        """Set task count.

        Args:
            count (int): x
        """
        if isinstance(count, int):
            self.task_count = count
        else:
            raise Exception("set_task_count: requires input of type int")


    def set_update_interval(self, val):
        """Set update interval for master_update function.

        Args:
            val (int): x
        """
        if isinstance(val, int):
            self.update_interval = val
        else:
            raise Exception("set_update_interval: requires input of type int")


    def set_general_init(self, input_function):
        """Set general_init function.

        Args:
            input_function (func): x
        """
        if hasattr(input_function, '__call__'):
            self.general_init = types.MethodType(input_function, self)
        else:
            raise Exception("set_general_init: requires input to be a function")


    def set_master_init(self, input_function):
        """Set master_init function.

        Args:
            input_function (func): x
        """
        if hasattr(input_function, '__call__'):
            self.master_init = types.MethodType(input_function, self)
        else:
            raise Exception("set_master_init: requires input to be a function")


    def set_master_update(self, input_function):
        """Set master_update function.

        Args:
            input_function (func): x
        """
        if hasattr(input_function, '__call__'):
            self.master_update = types.MethodType(input_function, self)
            self.use_master_update = True
        else:
            raise Exception("set_master_update: requires input to be a function")


    def set_master_process(self, input_function):
        """Set master_process function.

        Args:
            input_function (func): x
        """
        if hasattr(input_function, '__call__'):
            self.master_process = types.MethodType(input_function, self)
        else:
            raise Exception("set_master_process: requires input to be a function")


    def set_master_final(self, input_function):
        """Set master_final function.

        Args:
            input_function (func): x
        """
        if hasattr(input_function, '__call__'):
            self.master_final = types.MethodType(input_function, self)
        else:
            raise Exception("set_master_final: requires input to be a function")


    def set_worker_job(self, input_function):
        """Set worker_job function.

        Args:
            input_function (func): x
        """
        if hasattr(input_function, '__call__'):
            self.worker_job = types.MethodType(input_function, self)
        else:
            raise Exception("set_worker_job: requires input to be a function")


    def set_get_task_data(self, input_function):
        """Set get_task_data function.

        Args:
            input_function (func): x
        """
        if hasattr(input_function, '__call__'):
            self.get_task_data = types.MethodType(input_function, self)
        else:
            raise Exception("set_get_task_data: requires input to be a function")


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


    def master_update(self):
        """Template only.

        Should be replaced by user created function using
            set_master_update method.

        Run by master during intervals determined by `update_interval`.
            Only runs when using parallel processing method.
        No args or return value.
        """
        pass


    def master_process(self, worker_result):
        """Template only.

        Should be replaced by user created function using
            set_master_process method

        Run by master only during processing for each task received
            from a worker.
        No return value.

        Args:
            value (str): x
        """
        self.master_data.append(worker_result)


    def master_final(self):
        """Template only.

        Should be replaced by user created function using
            set_master_final method

        Run by master only at end of processing after all tasks have
            been completed by workers.
        No args or return value.
        """
        master_data_stack = self.master_data


    def worker_job(self, task_index, task_data):
        """Template only.

        Should be replaced by user created function using
            set_worker_job method

        Run by work after receiving a task from master.

        Args:
            task_data (int): x

        Returns:
            results: object to be passed back from worker to master
        """
        worker_tagline = "Worker {0} | Task {1} - ".format(self.rank, task_index)
        print worker_tagline

        results = task_data

        return results


    def get_task_data(self, task_index, source):
        """Template only.

        Should be replaced by user created function using
            set_get_task_data method

        Run by master when upon receiving a "ready" request from worker.
        Results returned will be passed to worker_job function after
        being sent to worker

        Args:
            task_index (int): x

        Returns:
            task_data: data to be sent from master to worker
        """
        task_data = self.task_list[task_index]
        return task_data


    def _worker_job(self, task_index, task_data):

        if not self.capture:
            return self.worker_job(task_index, task_data)

        else:
            try:
                with Capturing() as output:
                    results = self.worker_job(task_index, task_data)

                print '\n'.join(output)
                return results

            except:
                print '\n'.join(output)
                raise


    def run(self, allow_empty=False):
        """Run job in parallel or serial.
        """
        if self.rank == 0 and self.task_count == 0:
            msg = ("Task count = 0 on master node. "
                   "Either set a non-zero task count, "
                   "or make sure your task list "
                   "is being properly populated)")

            if allow_empty:
                warn(msg)
            else:
                raise Exception(msg)

        if self.parallel:
            self.run_parallel()
        else:
            self.run_serial()


    def run_serial(self):
        """Run job using set functions in serial."""
        if self.rank == 0:
            self.general_init()
            self.master_init()

            for i in range(self.task_count):
                task_data = self.get_task_data(i, 0)

                worker_data = self.worker_job(i, task_data)
                self.master_process(worker_data)

            self.master_final()


    def send_error(self):
        """Send error to workers/master (depending on source"""
        if self.rank == 0:
            for i in range(1, self.size):
                self.comm.send(None, dest=i, tag=self.tags.ERROR)
        else:
            self.comm.send(None, dest=0, tag=self.tags.ERROR)


    def run_parallel(self):
        """Run job using set functions in parallel."""
        self.general_init()

        self.comm.Barrier()

        if self.rank == 0:

            self.master_init()

            task_index = 0
            num_workers = self.size - 1
            closed_workers = 0
            err_status = 0

            worker_info_template = {
                'status': 'up',
                'current_task_index': None,
                'current_task_data': None,
                'task_index_history': []
            }
            self.worker_log = dict(zip(
                range(1, self.size),
                [deepcopy(worker_info_template) for i in range(1, self.size)]
            ))

            print "Master - starting with {0} workers".format(num_workers)

            last_update = time.time()

            # distribute work
            while closed_workers < num_workers:
                active_workers = num_workers - closed_workers

                if self.use_master_update:
                    req = self.comm.irecv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG)
                else:
                    worker_data = self.comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=self.status)

                while True:

                    if self.use_master_update:

                        if self.update_interval and time.time() - last_update > self.update_interval:
                            self.master_update()
                            last_update = time.time()

                        re = req.test(status=self.status)


                    if not self.use_master_update or re[0] != False:

                        if self.use_master_update:
                            worker_data = re[1]

                        source = self.status.Get_source()
                        tag = self.status.Get_tag()

                        if tag == self.tags.READY:

                            if task_index < self.task_count:

                                task_data = self.get_task_data(task_index, source)

                                if (isinstance(task_data, tuple)
                                    and len(task_data) == 3
                                    and task_data[0] == "error"):

                                    print ("Master - shutting down worker {1} "
                                           "with task {0} ({2})").format(
                                                task_index, source, task_data[2])

                                    self.comm.send(None, dest=source, tag=self.tags.EXIT)

                                else:
                                    print "Master - sending task {0} to worker {1}".format(
                                        task_index, source)

                                    task = (task_index, task_data)

                                    tmp_ctd = deepcopy(task_data)
                                    tmp_ctd['_id'] = str(tmp_ctd['_id'])

                                    self.worker_log[source]['current_task_index'] = task_index
                                    self.worker_log[source]['current_task_data'] = tmp_ctd


                                    self.comm.send(task, dest=source, tag=self.tags.START)

                                    task_index += 1

                            else:
                                self.comm.send(None, dest=source, tag=self.tags.EXIT)

                        elif tag == self.tags.DONE:
                            worker_task_index = self.worker_log[source]['current_task_index']
                            self.worker_log[source]['current_task_index'] = None
                            self.worker_log[source]['current_task_data'] = None
                            self.worker_log[source]['task_index_history'].append(worker_task_index)
                            print "Master - received Task {0} data from Worker {1}".format(worker_task_index, source)
                            self.master_process(worker_data)

                        elif tag == self.tags.EXIT:
                            print "Master - worker {0} exited. ({1})".format(
                                source, active_workers - 1)
                            closed_workers += 1

                        elif tag == self.tags.ERROR:
                            print "Master - error reported by worker {0}.".format(source)
                            # broadcast error to all workers
                            self.send_error()
                            err_status = 1

                        # finish handling nonblocking receive and return to
                        # main worker communication loop to wait for next work message
                        break


            if err_status == 0:
                print "Master - processing results"
                self.master_final()

            else:
                print "Master - terminating due to worker error."

            if self.print_worker_log:
                print "Worker Log:"
                print json.dumps(
                    self.worker_log,
                    indent=4, separators=(",", ":"))

        else:
            # Worker processes execute code below
            print "Worker {0} - rank {0} on {1}.".format(self.rank, self.processor_name)
            while True:
                self.comm.send(None, dest=0, tag=self.tags.READY)
                master_data = self.comm.recv(source=0,
                                             tag=MPI.ANY_TAG,
                                             status=self.status)

                tag = self.status.Get_tag()

                if tag == self.tags.START:

                    task_index, task_data = master_data

                    try:
                        worker_result = self._worker_job(task_index, task_data)

                    except Exception as e:
                        print "Worker ({0}) - encountered error on Task {1}".format(self.rank, task_index)
                        # print e.encode('utf-8')
                        traceback.print_exc()
                        self.send_error()

                    else:
                        # send worker_result back to master (master_process function)
                        self.comm.send(worker_result, dest=0, tag=self.tags.DONE)


                elif tag == self.tags.EXIT:
                    self.comm.send(None, dest=0, tag=self.tags.EXIT)
                    break

                elif tag == self.tags.ERROR:
                    print ("Worker ({0}) - error message from Master. "
                           "Shutting down.").format(self.rank)
                    # confirm error message received and exit
                    self.comm.send(None, dest=0, tag=self.tags.EXIT)
                    break


