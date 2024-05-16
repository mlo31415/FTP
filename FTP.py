from __future__ import annotations
from typing import Optional

import ftplib
from ftplib import error_perm
import json
import os
import tempfile
import io

from Log import Log, LogFlush
from HelpersPackage import TimestampFilename


class FTP:
    g_ftp: ftplib.FTP=None      # A single FTP link for all instances of the class
    g_curdirpath: str="/"
    g_credentials: dict={}      # Saves the credentials for reconnection if the server times out
    g_dologging: bool=True      # Turn on logging of useful debugging information


    # ---------------------------------------------
    def OpenConnection(self, credentialsFilePath: str) -> bool:
        with open(credentialsFilePath) as f:
            FTP.g_credentials=json.loads(f.read())
        return self.Reconnect()     # Not exactly a reconnect, but close enough...


    #----------------------------------------------
    # Get the ID from the FTP login-in credentials
    def GetEditor(self) -> str:
        return FTP.g_credentials["ID"]


    #----------------------------------------------
    # A special Log which only writes when FTP has logging turned on.
    # Used for debugging messages, b not error messages
    def Log(self, s: str):
        if self.g_dologging:
            Log(s)


    # ---------------------------------------------
    # If we get a connection failure, reconnect tries to re-establish the connection and put the FTP object into a consistent state and then to restore the CWD
    def Reconnect(self) -> bool:
        self.Log("Reconnect")
        if len(FTP.g_credentials) == 0:
            return False
        FTP.g_ftp=ftplib.FTP_TLS(host=FTP.g_credentials["host"], user=FTP.g_credentials["ID"], passwd=FTP.g_credentials["PW"])
        FTP.g_ftp.prot_p()

        # Now we need to restore the current working directory
        self.Log("Reconnect: g_ftp.cwd('/')")
        msg=self.g_ftp.cwd("/")
        self.Log(msg)
        ret=msg.startswith("250 OK.")
        if not ret:
            Log("FTP.Reconnect failed")
            return False

        self.Log("Reconnect: successful. Change directory to "+FTP.g_curdirpath)
        olddir=FTP.g_curdirpath
        FTP.g_curdirpath="/"
        self.SetDirectory(olddir)

        return True


    # ---------------------------------------------
    # Update the saved current working directory path
    # If the input is an absolute path, just use it (removing any trailing filename)
    # If it's a relative move, compute the new wd path
    def UpdateCurpath(self, newdir: str) -> None:
        self.Log("UpdateCurpath from "+FTP.g_curdirpath+"  with cwd('"+newdir+"')")
        if newdir[0] == "/":    # Absolute directory move
            FTP.g_curdirpath=newdir
        elif newdir == "..":    # Relative move up one directory
            #TODO: Note that we don't handle things like "../.." yet
            if FTP.g_curdirpath != "/":     # If we're already at the top, we stay put.
                head, _=os.path.split(FTP.g_curdirpath)    # But we're not, so we slice off the last directory in the saved wd path
                FTP.g_curdirpath=head
        else:
            # What's left is a CD downwards
            if FTP.g_curdirpath == "/":
                FTP.g_curdirpath+=newdir
            else:
                FTP.g_curdirpath+="/"+newdir


    #---------------------------------------------
    # Given a full path "/xxx/yyy/zzz" or a single child directory name (no slashes), change to that directory
    def CWD(self, newdir: str) -> bool:
        wd=self.PWD()
        self.Log("**CWD from '"+wd+"' to '"+newdir+"'")
        if wd == newdir:
            self.Log("  Already there!")
            return True

        try:
            msg=self.g_ftp.cwd(newdir)
        except Exception as e:
            self.Log("FTP.CWD(): FTP connection failure. Exception="+str(e))
            if not self.Reconnect():
                return False
            msg=self.g_ftp.cwd(newdir)

        self.Log(msg)
        ret=msg.startswith("250 OK.")
        if ret:
            self.UpdateCurpath(newdir)
        self.PWD()
        return ret


    # ---------------------------------------------
    # Make a new child directory named <newdir> in the current directory
    def MKD(self, newdir: str) -> bool:
        self.Log("**make directory: '"+newdir+"'")
        try:
            msg=self.g_ftp.mkd(newdir)
        except Exception as e:
            Log("FTP.MKD(): FTP connection failure. Exception="+str(e))
            if not self.Reconnect():
                return False
            msg=self.g_ftp.mkd(newdir)
        self.Log(msg+"\n")
        return msg == newdir or msg.startswith("250 ") or msg.startswith("257 ")     # Web doc shows all three as possible.


    # ---------------------------------------------
    def DeleteFile(self, fname: str) -> bool:
        self.Log("**delete file: '"+fname+"'")
        if len(fname.strip()) == 0:
            Log("FTP.DeleteFile(): filename not supplied.")
            LogFlush()
            assert False

        if not self.FileExists(fname):
            Log("FTP.DeleteFile: '"+fname+"' does not exist.")
            return True

        try:
            msg=self.g_ftp.delete(fname)
        except Exception as e:
            Log("FTP connection failure. Exception="+str(e))
            if not self.Reconnect():
                return False
            msg=self.g_ftp.delete(fname)
        self.Log(msg+"\n")
        return msg.startswith("250 ")


    # ---------------------------------------------
    def Rename(self, oldname: str, newname: str) -> bool:
        self.Log(f"**rename file: '{oldname}'  as  '{newname}'")
        if len(oldname.strip()) == 0 or len(newname.strip()) == 0:
            Log("FTP.Rename(): oldname or newname not supplied. Probably irrecoverable, so exiting program.")
            LogFlush()
            assert False

        if not self.FileExists(oldname):
            Log(f"FTP.Rename: '{oldname}' does not exist.")
            return False

        try:
            msg=self.g_ftp.rename(oldname, newname)
        except Exception as e:
            Log(f"FTP.Rename: FTP connection failure. Exception={e}")
            if not self.Reconnect():
                return False
            msg=self.g_ftp.rename(oldname, newname)
        self.Log(msg+"\n")
        return msg.startswith("250 ")


    # ---------------------------------------------
    # Delete a leaf-node directory and any files and empty directories it contains.
    # Note that since this does not delete recursively, the contents of any subdirectories must be deleted first.
    def DeleteDir(self, dirname: str) -> bool:
        self.Log("**delete directory: '"+dirname+"'")
        if len(dirname.strip()) == 0:
            Log("FTP.DeleteDir(): dirname not supplied.")
            LogFlush()
            assert False        # This should never happen.
        if dirname == "/":
            Log("FTP.DeleteDir(): Attempt to delete root -- forbidden")
            assert False

        if not self.FileExists(dirname):
            Log(f"FTP.DeleteDir(): '{dirname}' does not exist.")
            return True

        # The first step is to delete any files it contains
        files=self.Nlst(dirname)
        for file in files:
            self.DeleteFile(file)

        try:
            msg=self.g_ftp.rmd(dirname)
        except Exception as e:
            Log(f"FTP.DeleteDir(): FTP connection failure. Exception={e}")
            if not self.Reconnect():
                return False
            msg=self.g_ftp.rmd(dirname)
        self.Log(msg+"\n")
        return msg.startswith("250 ")


    #----------------------------------------------
    # Compare two paths for equality.  We ignore differences in trailing "/"
    def ComparePaths(self, p1: str, p2: str) -> bool:
        # Make sure that there is a trailing "/" before comparing
        if len(p1) == 0 or p1[-1] != "/":
            p1+="/"
        if len(p2) == 0 or p2[-1] != "/":
            p2+="/"
        return p1 == p2


    # ---------------------------------------------
    # Returns the full path to the current directory as a string
    def PWD(self) -> str:
        try:
            dir=self.g_ftp.pwd()
        except Exception as e:
            Log("FTP.PWD(): FTP connection failure. Exception="+str(e))
            if not self.Reconnect():
                return ""
            dir=self.g_ftp.pwd()
        self.Log("pwd is '"+dir+"'")

        # Check to see if this matches what self._curdirpath thinks it ought to
        lead, tail=os.path.split(FTP.g_curdirpath)
        Log(f"FTP.PWD(): {lead=}  {tail=}")
        if not self.ComparePaths(FTP.g_curdirpath,  dir):
            Log(f"FTP.PWD(): error detected -- self._curdirpath='{FTP.g_curdirpath}' and pwd returns '{dir}'")
            Log("Probably irrecoverable, so exiting program.")
            assert False

        return dir


    # ---------------------------------------------
    # Given a complete path of the form "/xxx/yyy/zzz" (note leading "/"and no trailing "/"), or a relative path of the form "xxx" (note no slashes) does it exist?
    def PathExists(self, dirPath: str) -> bool:
        dirPath=dirPath.strip()
        if len(dirPath) == 0:
            return False

        # Handle the case where we're looking at "/xxx", a folder at root level.
        if dirPath[0] == "/":
            if not self.CWD("/"):
                return False
            if dirPath[1:] == "":
                return True     # We asked for "/" with no file, so no need to check fpor file
            return self.FileExists(dirPath[1:])

        # Now deal with more complex paths
        path=dirPath.split("/")
        if len(path) == 0:
            return self.FileExists(dirPath)

        end=path[-1]    # The last element of the path
        rest="/".join(path[:-1])    # The beginning of the path
        if len(rest) > 0:
            self.CWD(rest)
        if end == "":
            return True
        return self.FileExists(end)


    # ---------------------------------------------
    # Given a filename (possibly includeing a complete path), does the file exist.  Note that a directory is treated as a file.
    def FileExists(self, filedir: str) -> bool:
        if self.g_dologging:
            Log("Does '"+filedir+"' exist?", noNewLine=True)
        if filedir == "/":
            self.Log("  --> Yes, it always exists")
            return True     # "/" always exists

        # Split the filedir into path+file
        path=""
        if "/" in filedir:
            path="/".join(filedir.split("/")[:-1])
            filedir=filedir.split("/")[-1]

        # Make sure we're at the path
        if len(path) > 0:
            if not self.PathExists(path):
                return False
            self.CWD(path)

        try:
            if filedir in self.g_ftp.nlst():
                self.Log("  --> yes")
                return True
            self.Log("'  --> no, it does not exist")
            return False
        except:
            Log("'FTP.FileExists(): FTP failure: retrying")
            if not self.Reconnect():
                return False
            return self.FileExists(filedir)


    #-------------------------------
    # Make newdir (which may be a full path) the current working directory.  It (and the whole chain leading to it) may optionally be created if it does not exist.
    # Setting Create=True allows the creation of new directories as needed
    # Newdir can be a whole path starting with "/" or a path relative to the current directory if it doesn't start with a "/"
    def SetDirectory(self, newdir: str, Create: bool=False) -> bool:
        self.Log("**SetDirectory: "+newdir)

        # Split newdir into components
        if newdir is None or len(newdir) == 0:
            return True

        # If we've been given an absolte path and we're already there, return
        if newdir[0] == "/" and newdir == self.g_curdirpath:
            self.Log("SetDirectory: already there with an absolute path")
            return True

        components=[]
        if newdir[0] == "/":
            components.append("/")
            newdir=newdir[1:]
        components.extend(newdir.split("/"))
        components=[c.strip() for c in components if len(c) > 0]

        # Now walk the component list
        for component in components:
            # Does the directory exist?
            if not self.FileExists(component):
                # If not, are we allowed to create it"
                if not Create:
                    Log("FTP.SetDirectory(): called for a non-existant directory with create=False")
                    return False
                if not self.MKD(component):
                    Log("FTP.SetDirectory(): mkd failed...bailing out...")
                    return False

            # Now cwd to it.
            if not self.CWD(component):
                Log("FTP.SetDirectory(): cwd failed...bailing out...")
                return False

        return True


    #-------------------------------
    # Copy the string s to fanac.org as a file in the current directory named fname.
    def PutString(self, fname: str, s: str) -> bool:
        if self.g_ftp is None:
            Log("FTP.PutString(): FTP not initialized")
            return False

        with tempfile.TemporaryFile() as f:

            # Save the string as a local temporary file, then rewind so it can be read
            f.write(bytes(s, 'utf-8'))
            f.seek(0)

            self.Log("STOR "+fname+"  from "+f.name)
            try:
                self.Log(self.g_ftp.storbinary("STOR "+fname, f))
            except Exception as e:
                Log(f"FTP.PutString(): FTP connection failure. Exception={e}")
                if not self.Reconnect():
                    return False
                self.Log(self.g_ftp.storbinary("STOR "+fname, f))
            return True


    #-------------------------------
    # Append the string s to file fname on fanac.org in the current directory
    def AppendString(self, fname: str, s: str) -> bool:
        if self.g_ftp is None:
            Log("FTP.AppendString(): FTP not initialized")
            return False

        with tempfile.TemporaryFile() as f:

            # Save the string as a local temporary file, then rewind so it can be read
            f.write(bytes(s, 'utf-8'))
            f.seek(0)

            self.Log("STOR "+fname+"  from "+f.name)
            try:
                self.Log(self.g_ftp.storbinary("APPE "+fname, f))
            except Exception as e:
                Log(f"FTP.AppendString(): FTP connection failure. Exception={e}")
                if not self.Reconnect():
                    return False
                self.Log(self.g_ftp.storbinary("APPE "+fname, f))
            return True


    #-------------------------------
    # Copy the string s to fanac.org as a file <fname> in directory <directory>, creating directories as needed.
    def PutFileAsString(self, directory: str, fname: str, s: str, create: bool=False) -> bool:
        if not FTP().SetDirectory(directory, Create=create):
            Log("FTP.PutFieAsString(): Bailing out...")
            return False
        return FTP().PutString(fname, s)

    # -------------------------------
    # Return True if a message is recognized as an FTP success message; False otherwise
    def IsSuccess(self, ret: str) -> bool:
        successMessages=[
            "226-File successfully transferred",
        ]
        ret=ret.split("\n")[0]      # Just want the 1st line if there are many
        return any([x == ret for x in successMessages])


    #-------------------------------
    # Copy a file from one directory on the server to another
    def CopyFile(self, oldpathname: str, newpathname: str, filename: str, Create: bool=False) -> bool:
        return self.CopyAndRenameFile(oldpathname, filename, newpathname, Create=Create)


    #-------------------------------
    # Copy a file from one directory on the server to another. Rename the file if newfilename != ""
    def CopyAndRenameFile(self, oldpathname: str, oldfilename: str, newpathname: str, newfilename: str=None, Create: bool=False, IgnoreMissingFile: bool=False) -> bool:
        if self.g_ftp is None:
            Log("FTP.CopyAndRenameFile(): FTP not initialized", isError=True)
            return False

        Log(f"CopyAndRenameFile: {oldpathname=} {oldfilename=} {newpathname=} {newfilename=}")

        self.CWD(oldpathname)

        # The lambda callback in retrbinary will accumulate bytes here
        temp: bytearray=bytearray(0)

        self.Log(f"RETR {oldfilename} from {oldpathname}")
        ret="No message returned by retrbinary()"
        try:
            ret=self.g_ftp.retrbinary(f"RETR {oldfilename}", lambda data: temp.extend(data))
            self.Log(ret)
        except error_perm as e:
            Log(ret)
            Log(f"FTP.CopyAndRenameFile().retrbinary(): Exception={e}", isError=True)
            if not self.Reconnect():
                if IgnoreMissingFile:
                    return True
                return False
            ret=self.g_ftp.retrbinary(f"RETR {oldfilename}", lambda data: temp.extend(data))
            self.Log(ret)

        if not self.IsSuccess(ret):
            Log(ret, isError=True)
            Log("FTP.CopyAndRenameFile(): retrbinary failed", isError=True)
            return False

        # Upload the file we just downloaded to the new directory, renaming it if specified.
        # The new directory must already have been created
        if not self.PathExists(newpathname):
            Log(f"FTP.CopyAndRenameFile(): newpathname='{newpathname}' not found", isError=True)
            if not Create:
                return False
            self.MKD(newpathname)
        self.CWD(newpathname)

        if newfilename is None:
            newfilename=oldfilename

        try:
            ret=self.g_ftp.storbinary(f"STOR {newfilename}", io.BytesIO(temp))
            self.Log(ret)
        except Exception as e:
            Log(f"FTP.CopyAndRenameFile().PutFile(): FTP connection failure. Exception={e}")
            if not self.Reconnect():
                return False
            ret=self.g_ftp.storbinary(f"STOR {newfilename}", io.BytesIO(temp))
            self.Log(ret)
        return True


    #-------------------------------
    # Make a timestamped copy of a file on the server
    def BackupServerFile(self, pathname) -> bool:
        path, filename=os.path.split(pathname)
        if not FTP().SetDirectory(path, Create=False):
            Log(f"FTP.BackupServerFile(): Could not set directory to {path}")
            return False
        return FTP().CopyAndRenameFile(path, filename, path, TimestampFilename(filename))


    #-------------------------------
    # Copy the local file fname to fanac.org in the current directory and with the same name
    def PutFile(self, pathname: str, toname: str) -> bool:
        if self.g_ftp is None:
            Log("FTP.PutFile(): FTP not initialized")
            return False

        self.Log("STOR "+toname+"  from "+pathname)
        try:
            with open(pathname, "rb") as f:
                try:
                    self.Log(self.g_ftp.storbinary("STOR "+toname, f))
                except Exception as e:
                    Log("FTP.PutFile(): FTP connection failure. Exception="+str(e))
                    if not self.Reconnect():
                        return False
                    self.Log(self.g_ftp.storbinary("STOR "+toname, f))
        except Exception as e:
            Log(f"FTP.PutFile(): Exception on Open('{pathname}', 'rb') ")
            Log(str(e))
        return True


    #-------------------------------
    # Download the ascii file named fname in the current directory on fanac.org into a string
    def GetAsString(self, fname: str) -> Optional[str]:
        if self.g_ftp is None:
            Log("FTP.GetAsString(): FTP not initialized")
            return None

        fd=tempfile.TemporaryDirectory()
        self.Log("RETR "+fname+"  to "+fd.name)
        if not self.FileExists(fname):
            Log(f"FTP.GetAsString(): {fname} does not exist.")
            fd.cleanup()
            return None
        # Download the file into the temporary file
        tempfname=os.path.join(fd.name, "tempfile")
        f=open(tempfname, "wb+")
        try:
            msg=self.g_ftp.retrbinary("RETR "+fname, f.write)
        except Exception as e:
            Log(f"FTP.GetAsString(): FTP connection failure. Exception={e}")
            if not self.Reconnect():
                fd.cleanup()
                return None
            msg=self.g_ftp.retrbinary("RETR "+fname, f.write)
        self.Log(msg)
        if not msg.startswith("226-File successfully transferred"):
            Log("FTP.GetAsString(): failed")
            fd.cleanup()
            return None

        with open(tempfname, "r") as f:
            out=f.readlines()
        out="/n".join(out)
        fd.cleanup()
        return out


    #-------------------------------
    def GetFileAsString(self, directory: str, fname: str) -> Optional[str]:
        if not self.SetDirectory(directory):
            Log("GetFileAsString(): Bailing out...")
            return None
        s=FTP().GetAsString(fname)
        if s is None:
            Log(f"FTP.GetFileAsString(): Could not load {directory}/{fname}")
        return s


    #-------------------------------
    def Nlst(self, directory: str) -> list[str]:
        if self.g_ftp is None:
            Log("FTP.Nlst(): FTP not initialized")
            return []

        if not self.SetDirectory(directory):
            Log("FTP.Nlst(): Bailing out...")
            return []

        return [x for x in self.g_ftp.nlst() if x != "." and x != ".."] # Ignore the . and .. elements
