import sys


# functions for generating user prompts
class prompts():

    def __init__(self):

        self.interface = True


    # prompt to continue function
    def user_prompt_bool(self, question):
        valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}
        
        while True:
            sys.stdout.write(str(question) + " [y/n] \n> ")
            choice = raw_input().lower()

            if choice in valid:
                return valid[choice]
            else:
                sys.stdout.write("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")


    # prompt to use entered/existing input
    def user_prompt_use_input(self, conditional=None, statement=0, value=None):
        if not statement:
            if value == None:
               quit("Error - No statement or value given to user_prompt_use_input.")

            statement = "Use the following? : \"" + str(value) + "\""

        if conditional == None:
            conditional = self.user_prompt_bool(str(statement))

        if not conditional:
            redo = self.user_prompt_bool("Use a new input [y] or exit [n]?")

            if not redo:
               quit("No input given at open prompt.")

            return False

        return True


    # open ended user prompt
    def user_prompt_open(self, question, check, new_val=(0,0) ):

        while True:

            if not new_val[0]:
                raw_answer = new_val[1] 
                use_answer = True
            else:
                sys.stdout.write(question + " \n> ")
                raw_answer = raw_input()
                use_answer = self.user_prompt_use_input(value=raw_answer)

            if use_answer:
                valid, checked_val, error = check(raw_answer)
                
                if not valid:

                    if error != None:
                        error = "("+error+")"
                    else:
                        error = ""

                    if self.interface and not self.user_prompt_bool("Invalid input " + error + "\nUse a new answer [y] or exit [n]?"):
                        quit("No answer given at open prompt.")
                    elif not self.interface:
                        quit("Invalid automated input.")
  

                else:
                    return checked_val

