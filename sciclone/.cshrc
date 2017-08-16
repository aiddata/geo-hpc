#
#  Default .cshrc file for SciClone.
#  Don't change this stuff unless you know what you're doing.
#
#  Revision history:
#     10/31/2007 tom    - Completely new approach for mixed Linux/Solaris
#                         environment.
#     12/03/2007 tom    - Distinguish between solaris-sparc and solaris-opteron
#                         in platform-specific files.
#     05/13/2008 tom    - Add aliases for common "qstat" commands.
#     06/23/2011 tom    - Updated shell variables to reflect current
#                         filesystem configuration.
#     02/23/2012 tom    - Updates for RHEL 6.x systems.
#
#================== Do not remove the following line! ===================

source /usr/local/etc/sciclone.cshrc

#========================================================================
#
#  The following options can be modified based on personal preference.

#
#  csh options
#
set filec
set notify

#
#  tcsh options
#
if ($?tcsh) then
   unset autocorrect
   unset autologout
   unset correct
endif

#
#  Settings for interactive shells only
#
if ( $?prompt ) then
   if ($?tcsh) then
      set prompt = "%B%h [%m] %b"
   else
      set host = `hostname`
      set domain = `domainname`
      set host = `basename $host .$domain`
      set prompt = "! [$host] "
   endif
   set history = 50
   set savehist = 10
endif

#
#  system and application variables
#
setenv PAGER less
setenv PRINTER "unspecified"

#
#  useful variables
#
setenv x ~/scr                    # symlink to user's primary scratch dir
setenv x0 /sciclone/scr00/$user   # global scratch dir
setenv x1 /sciclone/scr01/$user   # global scratch dir
setenv x2 /sciclone/scr02/$user   # global scratch dir
setenv x10 /sciclone/scr10/$user  # global scratch dir
setenv x20 /sciclone/scr20/$user  # global scratch dir
setenv lx /local/scr/$user        # local scratch dirs on all nodes
setenv d10 /sciclone/data10/$user # global large-data directory
setenv d20 /sciclone/data20/$user # global large-data directory

#
#  useful aliases
#
alias h history
alias l 'ls -laF'
alias la 'ls -aF'
alias lc 'ls -CaFq'
alias lt 'ls -ltF'
alias qsa "qstat -a"                    # show all jobs
alias qsn "qstat -n \!*"                # show the nodes assigned to a job
alias qsq "qstat -a | grep ' Q '"       # show queued jobs
alias qsr "qstat -r"                    # show running jobs
alias qss "qstat -s -u $user"           # show status of my jobs
alias qsu "qstat -u $user"              # show my jobs

#
# Load OS-specific personal settings, if any
#
switch ($OSTYPE)

   case "solaris":

      switch ($ARCH)

         case "sun4u":
            if (-r $HOME/.cshrc.solaris-sparc) source $HOME/.cshrc.solaris-sparc
            breaksw

         case "i86pc":
            if (-r $HOME/.cshrc.solaris-opteron) source $HOME/.cshrc.solaris-opteron
            breaksw

      default:
         breaksw

      endsw
      breaksw

   case "linux":
      if (-r $HOME/.cshrc.$PLATFORM) source $HOME/.cshrc.$PLATFORM
      breaksw

   default:
      breaksw

endsw



# updates for geo framework and misc

alias q "qstat -nu $user"            # show nodes of my jobs
alias qva "qstat -na | grep va -B 1" # show nodes for jobs on vortex-alpha"

setenv PYTHONPATH ${PYTHONPATH}:/sciclone/aiddata10/REU/py_libs/lib/python2.7/site-packages
#setenv PYTHONPATH ${PYTHONPATH}:${HOME}/py_libs/lib/python2.7/site-packages

setenv VISUAL /usr/bin/nano
setenv EDITOR /usr/bin/nano
