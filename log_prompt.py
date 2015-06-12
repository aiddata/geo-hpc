import sys
import re

# functions for generating user prompts
class prompts():

    def quit(self, reason):
        sys.exit("Terminating script - "+str(reason)+"\n")


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


    def user_prompt_use_input(self, conditional=None, statement=0, value=None):
        if not statement:
            if value == None:
               self.quit("Error - No statement or value given to user_prompt_use_input.")

            statement = "Use the following? : \"" + str(value) + "\""

        if conditional == None:
            conditional = self.user_prompt_bool(str(statement))

        if not conditional:
            redo = self.user_prompt_bool("Use a new input [y] or exit [n]?")

            if not redo:
               self.quit("No input given at open prompt.")

            return False

        return True


    # open ended user prompt
    def user_prompt_open(self, question, check=0, error=0):
        
        if error:
            error = " ("+error+")"
        else:
            error = ""

        while True:
            sys.stdout.write(question + " \n> ")
            raw_answer = raw_input()

            use_answer = self.user_prompt_use_input(value=raw_answer)

            if use_answer and check:
                check_result = check(raw_answer)
                
                if type(check_result) != type(True) and len(check_result) == 2:
                    valid, answer = check_result
                else:
                    valid = check_result
                    answer = raw_answer

                if not valid:
                    redo_answer = self.user_prompt_bool("Invalid input" + error + "\nUse a new answer [y] or exit [n]?")

                    if not redo_answer:
                       self.quit("No answer given at open prompt.")

                else:
                    return answer

            elif use_answer:        
                return raw_answer


    def user_prompt_loop(self, struct, question, cont):
        result = []

        c = 0
        while True:
            sub_result = struct
            for k in sub_result.keys():
                sub_result[k] = self.user_prompt_open(str(question)+str(c)+" "+k+"?")
            
            result.append(sub_result)
            c += 1

            if not self.user_prompt_bool(str(cont)):
                return result