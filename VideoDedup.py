#!/usr/bin/env python3

#ToDo:


import sys
import os
import subprocess
import fnmatch
import shutil
import time
import random
from PIL import Image
import numpy
import pandas
import multiprocessing
# from multiprocessing import shared_memory
from multiprocessing.shared_memory import SharedMemory
from multiprocessing.managers import SharedMemoryManager

# from multiprocessing import set_start_method

# from multiprocessing import current_process
# import threading
import psycopg2
import psycopg2.extras
from pynput import keyboard
from config import config
import tracemalloc
import gc
import psutil

# Declarations
version = 'VideoDedup v3.4.007'
test = True
debug = 1
copyright = 0
cpttodo = 0
cptdone = 0
level = 0
fpsn = 1.0
threads = 3
limffmpeg = 99
maxdiff = 0
maxdiffuw = 0
minimg = 5
speed = 2
blksize = 999
parallel = False
renamed = False
noffmpeg = False
sameresult = False
nosrccp = False
delparam = False
stopaskedflag = False
# RAMoptimization = False
sqldb = ''
sqlbak = ''
step0 = False
step1 = False
step2 = False
step3 = False
srclst = []
pid = str(time.time())
logfile = 'log/VideoDedup.' + pid + '.log'
foldervideo = '.'
folderimg = '.'
folderrs = './rs/'
folderuw = './uw/'
mpdata = []
nbrecordscached = 0
txtred = '\033[0;31m'
txtgreen = '\033[0;32m'
txtblue = '\033[0;94m'
txtnocolor = '\033[0m'
jobcounter = 0
ckey00 = 0
ckey01 = 13238554390844451712152512706967568373366178310835508067696502068769761726817965031138958243566990544203465978209372017043226568506848757390608845972979336010917733431720962329755301155189638756012337352140791598028799977738496606961980141511392723170057732767286397005613388838322051907067178781420124649608953620587271960410893106261000486639197525063338214818008032229876309636871587925173899914867377780372227300152897200090110877488080674985642113769952928351076149768394879056872757190617791059112078007193363939328
ckey02 = 15129776317850042068329229771629254579998247925027976662021218659243659531545439858743498156192674422510200143835625965403855003139740610904337458463180015480587767986734692579649790046216908760909693678675125212978436623073096317203027693310270636510581280054417911056713493333402850521673499038496653810991486470822897078312079512216359979673447987480937536668822044680515092408040686214000921048374452311271366876013476718428320314447633503697904239024321144758065581630775360040384097028726472327368755552213683666943
ckey03 = 15121927007626601813066675049248571430983671982395241835845502665883547525896891189637339314066811770816198901022706694385709612086775776868585991336191549835301589722660440485595959055743622936388594215125915982529734335083115245910587330817042640482905873272678324714091132092698456870663789687017887150639258977335696957366988263628490772763453043832859804898308018650834297012365409364476970104239280461134118360610459076577547798700577290225063368210298855923045570904171082028909858136985579928977805765260364218368
records = []
maxleft = 0
scaninorder = False

# log messages to log file and to screen
def log(s='', threshold=1, file=''):
  s = duration(time.perf_counter() - perf) + ' ' + s
  if file == '':
    flog.write(s + '\n')
  else:
    flocal = open(file, 'a')
    flocal.write(s + '\n')
    flocal.close()
  if debug >= threshold: print(s)


def duration(d, dat=True):
  h = int(d // 3600)
  m = int((d - 3600 * h) // 60)
  s = d - 3600 * h - 60 * m
  if d > 3600:
    r = '{:02d}'.format(h) + ' h ' + '{:02d}'.format(m)
  elif d > 60:
    r = '{:02d}'.format(m) + ' m ' + '{:02.0f}'.format(s)
  else:
    r = '{:05.2f}'.format(s) + ' s'
  if dat:
    r = txtgreen + time.asctime(time.localtime(time.time())) + txtnocolor + ' ' + r
  return r


def sortoccurence(elem):
  return elem[0]


def shortname(fullname):
  # log('shortname(' + fullname + ')', 0)
  pend = len(fullname) - 1
  if fullname[pend:pend + 1] == '/':
    fullname = fullname[:pend]
    pend = pend - 1
  while (pend > 0) and (fullname[pend:pend + 1] != '/'):
    pend = pend - 1
  # log('shortname: pend=' + str(pend) + ' => ' + fullname[pend+1:], 0)
  return fullname[pend + 1:]


def pathtoshort(pat, racine):
  pt = 1
  pat = pat[len(racine)-1:]
  res = ''
  while pt < len(pat):
    if pat[pt] != '/': res = res + pat[pt]
    else: res = res + '_'
    pt = pt + 1
  # log(f"pathtoshort({pat}, {racine}) = {res}", 1)
  return res


def SameSrcNames():
  global srclst, renamed

  srclst = sorted(srclst, key=sortoccurence)
  log('Test of source with same name. len(srclst)=' + str(len(srclst)), 2)
  log('First one is ' + srclst[0][0], 2)
  prev = ['']
  firstrenamed = False
  for i in range(len(srclst)):
    # DEBUG : the prev file must be renamed also to avoid pointing on a bad image
    if srclst[i][0] == prev[0]:
      renamed = True
      log('File ' + srclst[i][1] + srclst[i][0] + ' is referenced multiples times. Renaming all of them.', 1)
      log('... renaming to ' + srclst[i][1] + str(i) + srclst[i][0], 1)
      if not (firstrenamed):
        log('File ' + prev[1] + prev[0] + ' is referenced multiples times. Renaming all of them.', 2)
        log('... renaming to ' + prev[1] + str(i - 1) + prev[0], 1)
        os.rename(prev[1] + prev[0], prev[1] + str(i - 1) + prev[0])
        firstrenamed = True
      os.rename(srclst[i][1] + srclst[i][0], srclst[i][1] + str(i) + srclst[i][0])
    else:
      firstrenamed = False
    prev = srclst[i]


def SQLexec(sql, displayresult=False, threshold=1):
  global conn, cur

  if len(sql) > 0:
    log(sql, threshold)
    try:
      cur.execute(sql)
    except (Exception, psycopg2.DatabaseError) as error:
      print('ERROR cannot execute: ' + sql)
      print(error)
      sys.exit(1)
    if displayresult:
      try:
        row = cur.fetchone()
        log(str(row), threshold)
        return row[0]
      except(Exception, psycopg2.DatabaseError) as error:
        print('ERROR cannot get result of: ' + sql)
        print(error)
        return 0


def SQLopen():
  global conn, cur

  SQLexec('CREATE TABLE IF NOT EXISTS timages (Id SERIAL NOT NULL PRIMARY KEY, tsdupe INTEGER, srcshortname TEXT, imgshortname TEXT, keyimg TEXT, dist00 INTEGER, dist01 INTEGER, dist02 INTEGER, dist03 INTEGER)', False, 2)
  SQLexec('CREATE TABLE IF NOT EXISTS timagesuw (Id SERIAL NOT NULL PRIMARY KEY, imgshortname TEXT, keyimg TEXT, dist00 INTEGER, dist01 INTEGER)', False, 2)
  SQLexec('CREATE TABLE IF NOT EXISTS tpairsuw (Id SERIAL NOT NULL PRIMARY KEY, srcshortname1 TEXT, srcshortname2 TEXT)', False, 2)
  SQLexec('CREATE TABLE IF NOT EXISTS timagedupes (idimage1 INTEGER, idimage2 INTEGER, dist INTEGER)', False, 2)
  SQLexec('CREATE TABLE IF NOT EXISTS tsources (srcshortname TEXT PRIMARY KEY, srcfld TEXT, imgfld TEXT)', False, 2)
  SQLexec('CREATE TABLE IF NOT EXISTS tmpimgpairs (dist INTEGER, Lid INTEGER, Lsrcshortname TEXT, Limgshortname TEXT, Rid INTEGER, Rsrcshortname TEXT, Rimgshortname TEXT);', False, 2)
  SQLexec('CREATE TABLE IF NOT EXISTS tmppairsofsources (iddupepair SERIAL NOT NULL PRIMARY KEY UNIQUE,Lsrcshortname TEXT,Rsrcshortname TEXT);', False, 2)
  SQLexec('CREATE TABLE IF NOT EXISTS tmpResult (iddupepair INTEGER, srcshortname TEXT, imgshortname TEXT);', False, 2)
  SQLexec('CREATE TABLE IF NOT EXISTS tparam (paramkey TEXT NOT NULL PRIMARY KEY, paramvalue TEXT);', False, 2)
  SQLexec("INSERT INTO tparam (paramkey, paramvalue) VALUES('scaninorder','-1') ON CONFLICT (paramkey) DO NOTHING;", False, 2)

  # SQLexec('CREATE INDEX IF NOT EXISTS idx_timages_dist00       ON public.timages USING btree (dist00 ASC NULLS LAST) TABLESPACE pg_default;', False, 2)
  # SQLexec('CREATE INDEX IF NOT EXISTS idx_timages_dist01       ON public.timages USING btree (dist01 ASC NULLS LAST) TABLESPACE pg_default;', False, 2)

  SQLexec('CREATE INDEX IF NOT EXISTS idx_timages_comp         ON public.timages USING btree (dist00, tsdupe, dist01, dist02, dist03) TABLESPACE pg_default;', False, 2)
  SQLexec('CREATE INDEX IF NOT EXISTS idx_timages_srcshortname ON public.timages USING btree (srcshortname, imgshortname ASC NULLS LAST) TABLESPACE pg_default;', False, 2)

  # SQLexec('CREATE INDEX IF NOT EXISTS idximagesuw_dist        ON public.timagesuw (dist00 ASC, dist01 ASC)', False, 2)
  conn.commit()


def SQLscript(filename, threshold=1):
  global conn

  def reqform(sqltxt):
    while (len(sqltxt) > 0) and ((sqltxt[-1] == '\n') or (sqltxt[-1] == ';')):
      # log(sqltxt + ' -> ' + sqltxt[:-1], 1)
      sqltxt = sqltxt[:-1]
    return sqltxt

  # log('SQLscript start ' + filename, 1)
  locperf = time.time()
  ok = True
  scrcur = conn.cursor()
  if ok and os.path.exists(filename):
    if os.path.getsize(filename) < 3:
      log('Empty file. Removing ' + filename, threshold)
      os.remove(filename)
    else:
      fp = open(filename, 'r')
      sql = reqform(fp.readline())
      linenb = 1
      if (sql[:6] == 'INSERT') or (sql[:6] == 'UPDATE')  or (sql[:6] == 'DELETE'):
        # log('Loading ' + filename + ' in sql', 3)
        lastreq = time.time()
        # log(filename + ' line #' + f"{linenb:_d} " + ' Start: ' + sql[:55] + ' (' + str(len(sql)) + ')', 3)
        try:
          if len(sql) > 0:
            scrcur.execute(sql)
        except (Exception, psycopg2.DatabaseError) as error:
          print(error)
          ok = False
        #   log(txtred + 'ERROR: ' + txtnocolor + 'SQL query of line ' + str(linenb) + ' malformed. Removing it', 0)
        #   print(sql)
        while sql and ok:
          dur = time.time() - locperf
          durreq = time.time() - lastreq
          if (dur > 30 * (threshold + 1)) or (durreq > 2 * (threshold + 1)):
            log('SQLscript ' + filename + ' line #' + f"{linenb:_d} " + ': ' + sql[:55] + ' [' + str(len(sql)) + '] executed in ' + duration(durreq, False) + ', total time: ' + duration(dur, False), 1)
          # if (linenb % 1000000 == 0): log(f"{linenb:_d}" + ' lines executed yet. Current: ' + sql, threshold)
          # log(filename + ' line #' + f"{linenb:_d} " + ' DONE: ' + sql[:55] + ' [' + str(len(sql)) + '] executed in ' + duration(durreq, False) + ', total time: ' + duration(dur, False), 3)
          sql = reqform(fp.readline())
          linenb = linenb + 1

          lastreq = time.time()
          # log(filename + ' line #' + f"{linenb:_d} " + ' Start: ' + sql[:55] + ' (' + str(len(sql)) + ')', 3)
          try:
            if len(sql) > 0:
              scrcur.execute(sql)
              # time.sleep(0.001)
          except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            ok = False

        fp.close()
        # scrconn.commit()
        conn.commit()
        scrcur.close()
        # scrconn.close()
        log('Removing ' + filename, threshold)
        os.remove(filename)
      else:
        log(txtred + 'ERROR: ' + txtnocolor + 'Script ' + filename + ' is not starting by SQL instruction: ' + sql[:6], 0)

  dur = time.time() - locperf
  log('SQLscript(' + filename + '): in ' + duration(dur, False), 2)


def mediainfo(vfile):
  mediainfo = 1
  # s = 'convert ' + folder + '/' + file + '[160x160] -modulate 100,0 -blur 3x99 -normalize -equalize -resize 16x16 -threshold 50% ' + tmpmp + file +''
  s = '/usr/bin/bash /zpool/zdata/nassys/img.1-not-saved/fp/mediainfo.sh ' + vfile
  p = subprocess.Popen(s, stdout=subprocess.PIPE, shell=True)
  (output, err) = p.communicate()
  # log('mediainfo: Output=' + str(output), 3)
  if err == None:
    if len(output) > 3:
      tmp = float(output[:len(output) - 1]) * 0.001
      try:
        mediainfo = int(tmp / fpsn) - 1
      except:
        log('ERROR value too low in mediainfo(' + str(vfile) + '). ' + str(tmp) + ' / ' + str(fpsn), 0)
    else:
      log('ERROR mediainfo(' + str(vfile) + ') :', 0)
      print(output)
  return mediainfo


def distanceham(key1, key2):
  return bin(key1 ^ key2).count('1')


def IsVideo(extention=''):
  return ((extention == '.MP4') or (extention == '.AVI') or (extention == '.MOV') or (extention == '.M4V') or (extention == '.VOB')
          or (extention == '.MPG') or (extention == '.MPEG') or (extention == '.MKV') or (extention == '.WMV') or (extention == '.ASF')
          or (extention == '.FLV') or (extention == '.RM') or (extention == '.OGM') or (extention == '.M2TS') or (extention == '.RMVB'))


# def on_release(key):
#   global stopaskedflag
#   # print('{0} released'.format(key))
#   if key == keyboard.Key.esc:
#     # Stop listener
#     stopaskedflag = True


def StopAsked():
  global stopaskedflag
  # listener = keyboard.Listener(on_release=on_release)
  # listener.start()
  return stopaskedflag


def BoucleSupp(radical='', root=True):
  # Step1: remove images with no more source
  # executed only in clean mode
  global srclst

  if radical != '':
    if radical[-1] != '/': radical = radical + '/'
  if root:
    log('BoucleSupp(' + radical + ') will move images if source moved or delete it.', 1)

  ext = os.path.splitext(folderimg + radical)[1]
  ext = ext.upper()
  notvidimgfolder = not (IsVideo(ext))
  if os.path.isdir(folderimg + radical):
    fcount = 0
    for file in os.scandir(folderimg + radical):
      fcount = fcount + 1
      fname = file.name
      ext = os.path.splitext(fname)[1]
      ext = ext.upper()
      if file.is_dir():
        log('file = ' + fname + ', ext = ' + ext, 2)
        if IsVideo(ext):
          srcfilename = foldervideo + radical + fname
          if not (os.path.exists(srcfilename)):
            found = False
            for newsrc in srclst:
              if newsrc[0] == fname:
                found = True
                mvsrc = folderimg + radical + fname + '/'
                mvdst = newsrc[2] + fname + '/'
                log('Moving ' + mvsrc + ' to ' + mvdst, 0)
                try:
                  # print(newsrc[2])
                  # print(fname)
                  shutil.move(folderimg + radical + fname, newsrc[2])
                except:
                  log('' + txtred + 'Error' + txtnocolor + ' when moving image folder. Try to remove ' + folderimg + radical + fname, 0)
                if os.path.exists(folderimg + radical + fname):
                  log('deleting ' + folderimg + radical + fname, 0)
                  try:
                    shutil.rmtree(folderimg + radical + fname)
                  except:
                    log('' + txtred + 'Error' + txtnocolor + ' when deleting image folder ' + folderimg + radical + fname, 0)
            if not (found):
              log('' + txtred + 'Error' + txtnocolor + ' not found ' + fname + '. The file was deleted. Removing the image folder.', 1)
              shutil.rmtree(folderimg + radical + fname)
          else:
            if speed < 3:
              found = False
              for filejpeg in os.scandir(folderimg + radical + fname):
                if filejpeg.name[-4:] == '.jpg':
                  found = True
                  break
              if not(found):
                log('Delete because no .jpg image : ' + folderimg + radical + fname, 0)
                shutil.rmtree(folderimg + radical + fname)
        else:
          BoucleSupp(radical + fname, False)
      else:
        if notvidimgfolder:
          # print(folderimg + radical + file.name + ' -> ' + os.path.splitext(file)[1])
          if (file.name == 'fingerprint.fp') or (file.name == 'param.txt') or (os.path.splitext(file)[1] == '.src') or (os.path.splitext(file)[1] == '.jpg'):
            if radical[:8] != 'unwanted':
              log('Misplaced files. Deleting ' + folderimg + radical + file.name, 0)
              os.remove(folderimg + radical + file.name)

    if fcount == 0:
      log('Delete empty folder ' + folderimg + radical, 2)
      shutil.rmtree(folderimg + radical)


def BoucleCount(folderv='.', folderi='.', level=1):
  # Count source to do
  global cpttodo
  global srclst

  level = level + 1
  spacer = ''
  if debug > 1:
    for i in range(level): spacer = spacer + '  '
    log(spacer + '[ ' + folderv, 1)
  if os.path.isdir(folderv):
    if not (os.path.exists(folderi)):
      os.mkdir(folderi, mode=0o777)
    if folderv[-1] != '/': folderv = folderv + '/'
    if folderi[-1] != '/': folderi = folderi + '/'
    for file in os.scandir(folderv):
      fname = file.name
      ext = os.path.splitext(fname)[1]
      ext = ext.upper()
      if os.path.isdir(folderv + fname):
        BoucleCount(folderv + fname, folderi + fname, level + 1)
      elif IsVideo(ext):
        nameerror = False
        for c in fname:
          if c in [';', ' ']:
            nameerror = True
        if nameerror:
          log('ERROR in naming convention for ' + fname, 0)
          sys.exit(1)
        cpttodo = cpttodo + 1
        srclst.append([fname, folderv, folderi])
      elif not (ext == '.JPG' or ext == '.TXT' or ext == '.TXT~' or ext == '.SRC'):
        log(spacer + '  Not match : ' + folderv + fname, 2)

  if debug > 1:
    spacer = ''
    for i in range(level): spacer = spacer + '  '
    log(spacer + '  ' + folderv + ' count = ' + str(cpttodo) + ' ]', 1)
  level = level - 1


def calcfp(folder, file, display=False):
  result = -1
  if os.path.splitext(file)[1] == '.jpg':
    s = 'convert ' + folder + '/' + file + '[320x180] -gravity Center -crop 210x128+0-4 +repage -blur 3x99 -normalize -equalize -resize 32x18! -level 50%,50% ' + tmp + file + ''
    p = subprocess.Popen(s, stdout=subprocess.PIPE, shell=True)
    (output, err) = p.communicate()
    if err == None:
      if os.path.exists(tmp + file):
        if display:
          print('Temp cropped normalized images is store there: ' + tmp + file)
        im = Image.open(tmp + file)
        img = numpy.asarray(im)
        key = '0b'
        color = -1
        lgn = ''
        if img.shape == (18, 32, 3): color = 3
        if img.shape == (18, 32): color = 1
        if color == 3:
          for i in range(img.shape[0]):
            for j in range(img.shape[1]):
              for col in range(0, 3): # from 0 to 2
                if (img[i, j][col] < 128):
                  key = key + '0'
                else:
                  key = key + '1'
                if display:
                  if col == 0: lgn = lgn + txtred
                  if col == 1: lgn = lgn + txtgreen
                  if col == 2: lgn = lgn + txtblue
                  if (img[i, j][col] < 128): lgn = lgn + ' '
                  else:                      lgn = lgn + 'O'
            if display: lgn = lgn + '\n'
          result = int(key, 2)
        elif color == 1:
          for i in range(img.shape[0]):
            for j in range(img.shape[1]):
              if (img[i, j] < 128):
                key = key + '000'
              else:
                key = key + '111'
          result = int(key, 2)
        if color == -1:
          log(txtred + 'ERROR: ' + txtnocolor + 'tmp image is not coloured: ' + tmp + file, 0)
          log(str(img.shape), 0)
          sys.exit(1)
  else:
    if display:
      print('File not found: ' + folder + '/' + file)
  if display:
    lgn = lgn + txtnocolor
    print(lgn)
    print(key)
    print(result)
  return result


def DisplayImages():
  global listimages

  listkeyimg = []
  for im in listimages:
    file = shortname(im)
    long = len(shortname(im))
    listkeyimg.append(calcfp(im[:-long], file, True))
  print('Distance between ' + listimages[0] + ' and ' + listimages[1] + ' is ' + str(distanceham(listkeyimg[0], listkeyimg[1])))


def mpimagemagick(elt, queue):
  global fpsn, perf, tmp

  def mpcalcfp(folder, file):
    result = -1
    if os.path.splitext(file)[1] == '.jpg':
      # s = 'convert ' + folder + '/' + file + '[240x140] -modulate 100,0 -blur 3x99 -normalize -equalize -resize 24x14! -threshold 50% ' + tmpmp + file + ''
      s = 'convert ' + folder + '/' + file + '[320x180] -gravity Center -crop 210x128+0-4 +repage -blur 3x99 -normalize -equalize -resize 32x18! -level 50%,50% ' + tmpmp + file + ''
      p = subprocess.Popen(s, stdout=subprocess.PIPE, shell=True)
      (output, err) = p.communicate()
      if err == None:
        if os.path.exists(tmpmp + file):
          im = Image.open(tmpmp + file)
          img = numpy.asarray(im)
          key = '0b'
          color = -1
          if img.shape == (18, 32, 3): color = 3
          if img.shape == (18, 32): color = 1
          if color == 3:
            for i in range(img.shape[0]):
              for j in range(img.shape[1]):
                for col in range(0, 3):  # from 0 to 2
                  if (img[i, j][col] < 128):
                    key = key + '0'
                  else:
                    key = key + '1'
            result = int(key, 2)
          elif color == 1:
            for i in range(img.shape[0]):
              for j in range(img.shape[1]):
                if (img[i, j] < 128):
                  key = key + '000'
                else:
                  key = key + '111'
            result = int(key, 2)
          if color == -1:
            log(txtred + 'ERROR: ' + txtnocolor + 'tmp image is not coloured: ' + tmpmp + file, 0)
            log(str(img.shape), 0)
            sys.exit(1)
    return result

  def mpCreateFingerprint(folder=''):
    log('CreateFingerprint begin controls for ' + folder, 3)
    todo = True
    if folder[-1] != '/': folder = folder + '/'
    if not (os.path.isdir(folder)):
      log('' + txtred + 'ERROR: ' + txtnocolor + 'CreateFingerprint cannot run before ffmpeg for ' + folder, 0)
      todo = False

    if todo:
      fpram = []
      log('CreateFingerprint START for folder ' + folder, 3)
      srcshortname = shortname(folder)
      log('CreateFingerprint srcshortname= ' + srcshortname, 3)
      for file in sorted(os.listdir(folder)):
        # log('CreateFingerprint image = ' + file, 1)
        if todo and (os.path.splitext(file)[1].upper() == '.JPG'):
          # log('CreateFingerprint Call mpcalcfp(' + folder + ', ' + file + ')', 1)
          k = mpcalcfp(folder, file)
          if k == -1:
            todo = False
            log('' + txtred + 'ERROR: ' + txtnocolor + 'CreateFingerprint failed because mpcalcfp(' + folder + ', ' + file + ' gave result of -1', 1)
          else:
            fpram.append([file, k, distanceham(k, ckey00), distanceham(k, ckey01), distanceham(k, ckey02), distanceham(k, ckey03)])
            # log('CreateFingerprint add key of ' + str(k), 1)
      log('CreateFingerprint All jpeg parsed. Todo=' + str(todo), 3)

    if todo:
      thrconn = None
      try:
        params = config()
        thrconn = psycopg2.connect(**params)
        thrcur = thrconn.cursor()
      except (Exception, psycopg2.DatabaseError) as error:
        print(error)
      sql = ",".join("('%s', '%s', '%s', %s, %s, %s, %s, %s)" % (srcshortname, a, b, c, d, e, f, -1) for (a, b, c, d, e, f) in fpram)
      sql = 'INSERT INTO timages (srcshortname,imgshortname,keyimg,dist00,dist01,dist02,dist03,tsdupe) VALUES ' + sql + ';'
      try:
        thrcur.execute(sql)
        thrconn.commit()
        thrcur.close()
        thrconn.close()
      except (Exception, psycopg2.DatabaseError) as error:
        print(error)
      log('CreateFingerprint done for ' + folder, 3)

  (mpfolderi, mpfvideo, mpfile, mpargs, mps, mpcptdone, mpcpttodo, mptodoparam, mpminimgcount, mpminfo) = elt
  mpperf = time.time()
  todompimagemagick = True
  tmpmp = tmp[:-1] + '.' + str(mpcptdone) + '/'
  os.makedirs(tmpmp)
  log('Thread {}/{} '.format(mpcptdone, mpcpttodo) + 'Begin mpimagemagick. Call ffmpeg and/or image magick with folderi = ' + mpfolderi + ' file = ' + mpfile, 3)
  if not (os.path.exists(mpfolderi + mpfile + '/')):
    log('Thread {}/{} '.format(mpcptdone, mpcpttodo) + 'ERROR folder not exists ' + mpfolderi + mpfile, 0)
    # os.mkdir(mpfolderi + mpfile + '/', mode=0o777)
  else:
    if parallel:
      with open(mpfolderi + mpfile + '.run') as f:
        line = f.readline()
        line = line[:-1]
      if line != pid:
        log('    Thread ---------------------------------------------------------------------------------------------------', 0)
        log('    Thread --- Concurent ffmpeg run detected !', 0)
        log('    Thread --- .run flag for ' + mpfvideo + mpfile + ' Skip due to parallel mode ', 0)
        log('    Thread ---------------------------------------------------------------------------------------------------', 0)
        todompimagemagick = False
    if todompimagemagick:
      errflag = 0
      siz = os.path.getsize(mpfvideo) / 1048576
      if mptodoparam:
        # Call ffmpeg
        log('Thread {}/{} '.format(mpcptdone, mpcpttodo) + txtgreen + mps + txtnocolor, 2)
        p = subprocess.Popen(mpargs, stdout=subprocess.PIPE, close_fds=True)
        p.wait()
        (output, err) = p.communicate()
        # p.stdout.close()
        dur = time.time() - mpperf
        log('Thread {}/{} '.format(mpcptdone, mpcpttodo) + 'Duration ffmpeg: ' + duration(dur, False) + \
            ' for ' + str(round(siz, 0)) + ' Mb ' + txtgreen + '@ ' + str(round(threads * siz / dur * 0.0864, 2)) + ' Tb/day' + txtnocolor, 2)

      mpCreateFingerprint(mpfolderi + mpfile)

      # Create a file to store parameters
      # if os.path.exists(mpfolderi + mpfile + '/fingerprint.fp'):
      if os.path.exists(mpfolderi + mpfile):
        mpjpegcount = len(fnmatch.filter(os.listdir(mpfolderi + mpfile), '*.jpg'))
        if mpjpegcount >= mpminimgcount:
          f = open(mpfolderi + mpfile + '/param.txt', 'w')
          f.write('fps=1/' + str(fpsn) + '\n')
          f.close
        else:
          log('Thread {}/{} '.format(mpcptdone, mpcpttodo) + txtred + ' Error: ' + txtnocolor +
              f"Cannot create param.txt because not enough jpeg {mpjpegcount} / {mpminfo} = {round(mpjpegcount / mpminfo * 100)}% " + mpfolderi + mpfile, 0)
          fp = open(folderimg + 'lid_' + str(os.getpid()) + '.sqltmp', 'a')
          fp.write("DELETE FROM timages where srcshortname='" + mpfile + "';")
          fp.close()
          errflag = 2
      else:
        log('Thread {}/{} '.format(mpcptdone, mpcpttodo) + 'Cannot create param.txt because folder doesnt exists ' + mpfolderi + mpfile, 0)
        fp = open(folderimg + 'lid_' + str(os.getpid()) + '.sqltmp', 'a')
        fp.write("DELETE FROM timages where srcshortname='" + mpfile + "';")
        fp.close()
        errflag = 2
      dur = time.time() - mpperf
      if mptodoparam:
        msg = 'Duration ffmpeg + fingerprint: '
      else:
        msg = 'Duration fingerprint: '
      log('Thread {}/{} '.format(mpcptdone, mpcpttodo) + msg + duration(dur, False) + \
          ' for ' + str(round(siz, 0)) + ' Mb ' + txtgreen + '@ ' + str(round(threads * siz / dur * 0.0864, 2)) + ' Tb/day' + txtnocolor, errflag)
      log('Thread {}/{} '.format(mpcptdone, mpcpttodo) + mpfolderi + mpfile, 3)
      if parallel:
        os.remove(mpfolderi + mpfile + '.run')
      # else:
      #   log('Thread {}/{} '.format(mpcptdone, mpcpttodo) + mpfolderi + mpfile + '/fingerprint.fp doesnt exists.', 0)
    shutil.rmtree(tmpmp)


def OneFile(folderv, folderi, file, nbrecords):
  # Generate jpg images files for one source video file
  global cpttodo, cptdone, mpdata, pool, watcher, jobs, jobcounter

  def CreateSrc(folder):
    if not (os.path.exists(folder + '/' + file + '.src')):
      fp = open(folder + '/' + file + '.src', 'w')
      fp.write(file + '\n')
      fp.close

  # Initialization
  # print(f"OneFile folderi = {folderi}, file = {file}")
  fvideo = folderv + file
  fimg = folderi + file + '/img%05d.jpg'
  log('OneFile(' + folderv + ', ' + folderi + ', ' + file + ')', 2)
  if debug > 1: s = 'ffmpeg -i ' + fvideo + ' -vf fps=1/' + str(fpsn * (0.9 + random.random() / 4)) + ' ' + fimg + ''
  else:         s = 'ffmpeg -loglevel fatal -i ' + fvideo + ' -vf fps=1/' + str(fpsn * (0.9 + random.random() / 4)) + ' ' + fimg + ''
  args = []
  args.append('ffmpeg')
  if debug < 2:
    args.append('-loglevel')
    args.append('fatal')
  args.append('-i')
  args.append(fvideo)
  args.append('-vf')
  args.append('fps=1/' + str(fpsn * (0.9 + random.random() / 4)))
  args.append(fimg)
  folderi2 = folderi + file

  # Controls
  minfo = mediainfo(fvideo)
  minimgcount = limffmpeg * minfo / 100
  if not (os.path.exists(folderi2)):
    jpegcount = 0
  else:
    jpegcount = len(fnmatch.filter(os.listdir(folderi2), '*.jpg'))
  todoparam = not(os.path.exists(folderi2 + '/param.txt'))
  todofp = True
  if todoparam and (jpegcount >= minimgcount) and (jpegcount > 0):
    f = open(folderi2 + '/param.txt', 'w')
    f.write('fps=1/' + str(fpsn) + '\n')
    f.close
    todoparam = False
    log(folderi2 + f" Enough JPEG ({jpegcount} / {minfo}, {round(100 * jpegcount / minfo)}%), param.txt set", 1)
  if not(todoparam) and delparam:
    if (jpegcount < minimgcount) or (jpegcount == 0):
      todoparam = True
      log(folderi2 + f" Not enough jpeg ({jpegcount} / {minfo}, {round(100 * jpegcount / minfo)}%) and delparam set then remove param.txt and launch ffmpeg.")
      os.remove(folderi2 + '/param.txt')
  if noffmpeg: todoparam = False
  if todofp and not(os.path.exists(folderi2 + '/param.txt')):
    todofp = False
  if todofp and not(todoparam) and (speed < 3):
    if (nbrecords < (jpegcount * 0.99 - 1)) or (nbrecords == 0):
      nbrecords = SQLexec("SELECT COUNT(*) FROM timages WHERE srcshortname='" + file + "';", True, 2)
    if (nbrecords < (jpegcount * 0.99 - 1)) or (nbrecords == 0):
      log(folderi2 + ' Less records in timages than jpeg image files (' + str(nbrecords) + ' / ' + str(jpegcount) + '). Image Magick to run. Some will be re-erased in step 3 due to unwanted images.', 1)
    else:
      todofp = False
      log(folderi2 + ' Enough records in timages. ' + str(nbrecords) + ' records, ' + str(jpegcount) + ' jpeg files and ' + str(minfo) + ' media info need.', 3)

  # Lock mechanism for startover procedure and parralel mode
  if (todoparam or todofp) and parallel:
    log('lgn607 parallel is ON', 3)
    if os.path.exists(folderi2 + '.run'):
      todoparam = False
      todofp = False
      log('     --- .run flag for ' + folderv + file + ' Skip due to parallel mode ', 0)
    else:
      log('set ' + folderi2 + '.run flag', 2)
      f = open(folderi2 + '.run', 'w')
      f.write(pid + '\n')
      f.close

  # Execute
  if todoparam or todofp:
    if not (os.path.exists(folderi2)):
      os.makedirs(folderi2 + '/', mode=0o777)
    CreateSrc(folderi2)
    # fire off workers
    job = pool.apply_async(mpimagemagick, ([folderi, fvideo, file, args, s, cptdone, cpttodo, todoparam, minimgcount, minfo], queue))
    jobs.append(job)
    if not (queue.full):
      log('Execute 1 thread in the BoucleFichiers', 1)
      jobs[jobcounter].get()
      jobcounter = jobcounter + 1
    else:
      log('Queue full. Keep lock for later execution.', 3)

  cptdone = cptdone + 1
  if todoparam or todofp:
    log('' + txtgreen + str(cptdone) + ' / ' + str(cpttodo) + ' queued ...' + txtnocolor, 2)


def Parse():
  global cpttodo, cptdone, conn, cur, pool, watcher, jobs, jobcounter

  SQLexec('DROP INDEX IF EXISTS idx_timages_tsdupe;')   # Usefull for step 2 but slow down loading in step 1
  SQLexec('DROP INDEX IF EXISTS idx_timages_keyimg;')   # Usefull for step 3 but slow down loading in step 1
  SQLexec("UPDATE tparam SET paramvalue='-1' WHERE paramkey='scaninorder';", False, 2)
  SQLexec("INSERT INTO tparam (paramkey, paramvalue) VALUES('d0min','0') ON CONFLICT (paramkey) DO NOTHING;", False, 2)
  conn.commit()

  if speed == 1:
    log('Check jpeg count vs mediainfo duration. Can be skipped with speed>=2.')
    cur.execute("SELECT srcshortname, srcfld, imgfld FROM tsources WHERE srcshortname IN (SELECT DISTINCT srcshortname FROM timages)")
    row = cur.fetchone()
    while (row is not None):
      folderi2 = row[2] + row[0]
      minfo = mediainfo(row[1] + row[0])
      if not (os.path.exists(folderi2)):
        log(txtred + 'ERROR: ' + txtnocolor + ' Path not exists: ' + folderi2, 0)
      else:
        if os.path.exists(folderi2 + '/param.txt'):
          jpegcount = len(fnmatch.filter(os.listdir(folderi2), '*.jpg'))
          if (jpegcount < (limffmpeg * minfo / 100)) or (jpegcount == 0):
            if delparam:
              log('WARNING: Not enough jpeg for ' + txtgreen + folderi2 + txtnocolor + ' (' + str(jpegcount) + '/' + str(minfo) + ', ' + str(round(100 * jpegcount / minfo)) + '%). Delete param.txt.', 0)
              os.remove(folderi2 + '/param.txt')
            else:
              log('WARNING: Not enough jpeg for ' + txtgreen + folderi2 + txtnocolor + ' (' + str(jpegcount) + '/' + str(minfo) + ', ' + str(round(100 * jpegcount / minfo)) + '%). Use -delparam or delete param.txt manually to relaunch ffmpeg.', 0)
      row = cur.fetchone()
  if speed < 3:
    cur.execute("SELECT srcshortname, srcfld, imgfld, recordcount FROM tsources ORDER BY srcshortname;")
  else:
    cur.execute("SELECT srcshortname, srcfld, imgfld, recordcount FROM tsources WHERE srcshortname NOT IN (SELECT DISTINCT srcshortname FROM timages) ORDER BY srcshortname;")
  rows = cur.fetchall()
  # cptdone = cpttodo - cur.rowcount
  log(f"Key-images to compute or control: {cpttodo}", 1)

  pool = multiprocessing.Pool(int(0.6 * threads + 1.8))
  watcher = pool.apply_async(listener, args=(queue,), )
  jobs = []
  for row in rows:
    # if type(row[3]) == NoneType: OneFile(row[1], row[2], row[0], 0)
    if row[3] is None: OneFile(row[1], row[2], row[0], 0)
    else: OneFile(row[1], row[2], row[0], int(row[3]))

  log(txtgreen + 'Scan folder finished.' + txtnocolor + ' Wait for threads in queue to finish.', 0)
  while jobcounter < len(jobs):
    jobs[jobcounter].get()
    jobcounter = jobcounter + 1
  queue.put('kill')
  pool.close()
  pool.join()


def BoucleFichiers(folderv='.', folderi='.', action='parse', level=1):
  # Parse a single folder to call OneFile for source video files and BoucleFichier recursively if it'a a subfolder
  global mpdata

  level = level + 1
  spacer = ''
  if debug > 1:
    for i in range(level): spacer = spacer + '  '
    log(spacer + '[ ' + folderv, 0)
  if os.path.isdir(folderv):
    if not (os.path.exists(folderi)):
      os.mkdir(folderi, mode=0o777)
    if folderv[-1] != '/': folderv = folderv + '/'
    if folderi[-1] != '/': folderi = folderi + '/'

    for file in os.scandir(folderv):
      fname = file.name
      ext = os.path.splitext(fname)[1]
      ext = ext.upper()
      if file.is_dir():
        BoucleFichiers(folderv + fname, folderi + fname, action)
      elif IsVideo(ext):
        if action == 'parse':   OneFile(folderv, folderi, fname)
        # if action == 'fptosql': SQLscript(folderi + fname + '/fingerprint.fp')
      elif not (ext == '.JPG' or ext == '.TXT' or ext == '.TXT~'):
        log(spacer + '  Not match : ' + folderv + fname, 0)
  else:
    log('folderv = ' + folderv, 0)
    # if action == 'fptosql': SQLscript(folderi + '/fingerprint.fp')

  if debug > 1:
    spacer = ''
    for i in range(level): spacer = spacer + '  '
    log(spacer + folderv + ' ]', 0)
  level = level - 1


def CompleteDistanceOriginSql():
  log('CompleteDistanceOriginSql: Start compute distances for images', 0)
  cpt = 1
  while cpt > 0:
    cmds = []
    cpt = 0
    cur.execute('SELECT Id, keyimg, dist00 FROM timages WHERE dist03 IS NULL limit 1000000;')
    row = cur.fetchone()
    while row is not None:
      d0 = distanceham(int(row[1]), ckey00)
      d1 = distanceham(int(row[1]), ckey01)
      d2 = distanceham(int(row[1]), ckey02)
      d3 = distanceham(int(row[1]), ckey03)
      if d0 != row[2]:
        log(txtred + 'ERROR: ' + txtnocolor + 'dist00 change from ' + str(row[2]) + ' to ' + d0 + ' for Id=' + row[0], 0)
        sys.exit()
      cmds.append('UPDATE timages SET dist00=' + str(d0) + ', dist01=' + str(d1) + ', dist02=' + str(d2) + ', dist03=' + str(d3) + ' WHERE Id=' + str(row[0]) + ';')
      row = cur.fetchone()
      cpt = cpt + 1

    log('CompleteDistanceOriginSql: Compute done. Start update timages.', 0)
    cpt = 0
    try:
      for cmd in cmds:
        cur.execute(cmd)
        cpt = cpt + 1
      conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
      print(cmd)
      print(error)
    log('Done update distances for ' + str(cpt) + ' records of timages.')

  log('CompleteDistanceOriginSql: Start compute distances for unwanted images', 0)
  cpt = 1
  while cpt > 0:
    cmds = []
    cpt = 0
    cur.execute('SELECT Id, keyimg, dist00 FROM timagesuw WHERE dist03 IS NULL;')
    row = cur.fetchone()
    while row is not None:
      d0 = distanceham(int(row[1]), ckey00)
      d1 = distanceham(int(row[1]), ckey01)
      d2 = distanceham(int(row[1]), ckey02)
      d3 = distanceham(int(row[1]), ckey03)
      if d0 != row[2]:
        log(txtred + 'ERROR: ' + txtnocolor + 'dist00 change from ' + str(row[2]) + ' to ' + d0 + ' for Id=' + row[0], 0)
        sys.exit()
      cmds.append('UPDATE timagesuw SET dist00=' + str(d0) + ', dist01=' + str(d1) + ', dist02=' + str(d2) + ', dist03=' + str(d3) + ' WHERE Id=' + str(row[0]) + ';')
      row = cur.fetchone()
      cpt = cpt + 1

    log('CompleteDistanceOriginSql: Compute done. Start update timagesuw.', 0)
    cpt = 0
    try:
      for cmd in cmds:
        cur.execute(cmd)
        cpt = cpt + 1
      conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
      print(cmd)
      print(error)
    log('Done update distances for ' + str(cpt) + ' records of timagesuw.')


def listener(queue):
  '''listens for messages on the queue, writes to file. '''
  while 1:
    m = queue.get()
    if m == 'kill':
      log('Normal Queue killed since all work done.', 3)
      break


def mpCompareThread(leftrecords, queue):
  global scaninorder

  # All leftrecords in a single threads have the same dist00, dist01 and tsdupe => We can load possible rightrecords once
  exiterr = False
  mpconn = None
  try:
    params = config()
    mpconn = psycopg2.connect(**params)
    mpcur = mpconn.cursor()
  except (Exception, psycopg2.DatabaseError) as error:
    exiterr = True
    print(error)

  if not(exiterr):
    limram = round(99 - 100 / threads)
    ramusage = psutil.virtual_memory().percent
    if ramusage > limram:
      time.sleep(120)
      log(f"Thread {os.getpid()} neutralized 2mn due to RAM usage {ramusage}% > {limram}%. cpu: {psutil.cpu_percent(0.1)}%. You should diminish -threads value and/or force -blksize=n with smaller value.", 1)

    leftrecord = leftrecords[0]
    lastrecord = leftrecords[-1]
    if scaninorder:
      d0inf = leftrecord[5]  # Previous images have already been compared to this one in this same run
      d1inf = leftrecord[2]
      d2inf = leftrecord[6]
      d3inf = leftrecord[7]
    else:
      d0inf = leftrecord[5] - maxdiff  # Case of recent added image. Have to be compared to all around old ones.
      d1inf = leftrecord[2] - maxdiff
      d2inf = leftrecord[6] - maxdiff
      d3inf = leftrecord[7] - maxdiff
    d1max = lastrecord[2] + maxdiff
    d2max = lastrecord[6] + maxdiff
    d3max = lastrecord[7] + maxdiff

    if leftrecord[5] != lastrecord[5]:
      log(f"ERROR: dist00 not constant in this thread. First {leftrecord[5]} and Last {lastrecord[5]}:", 0)
      print(leftrecord)
      print(lastrecord)
      halt

    sqlin = "SELECT Id, tsdupe, dist01, srcshortname, keyimg, dist00, dist02, dist03 FROM timages WHERE dist00 >= " + str(d0inf) + " AND dist00 <= " + str(leftrecord[5] + maxdiff)
    sqlin = sqlin + " AND dist01 >= " + str(d1inf) + " AND dist01 <= " + str(d1max)
    optimdist2 = False
    optimdist3 = False
    if leftrecord[2] == lastrecord[2]:  # same dist01 also across the dataset for this thread
      sqlin = sqlin + " AND dist02 >= " + str(d2inf) + " AND dist02 <= " + str(d2max)
      optimdist2 = True
      if leftrecord[6] == lastrecord[6]:  # same dist02 also across the dataset for this thread, then dist03 is ordered. Else no filter on dist03 possible.
        sqlin = sqlin + " AND dist03 >= " + str(d3inf) + " AND dist03 <= " + str(d3max)
        optimdist3 = True
    t1 = time.time()
    mpcur.execute(sqlin)
    locdata = mpcur.fetchall()
    mpconn.close
    t2 = time.time()
    dursql = t2 - t1

    fp = open(folderimg + 'lid_' + str(os.getpid()) + '.sqltmp', 'a')
    nbfound = 0
    for leftrecord in leftrecords:
      sql = 'INSERT INTO timagedupes (idimage1,idimage2,dist) VALUES '
      for rightrecord in locdata:                            # Id, tsdupe, dist01, srcshortname, keyimg, dist00, dist02, dist03
        if rightrecord[3] != leftrecord[3]:
          dist = 9999                                        # Embeded Ifs instead of and/or to optimize tests in this very center of the loops
          if optimdist3:
            dist = distanceham(int(leftrecord[4]), int(rightrecord[4]))
          else:
            if optimdist2:
              if (rightrecord[7] > d3inf - 1) and (rightrecord[7] < d3max + 1):
                dist = distanceham(int(leftrecord[4]), int(rightrecord[4]))
            else:
              if (rightrecord[6] > d2inf - 1) and (rightrecord[6] < d2max + 1):
                if (rightrecord[7] > d3inf - 1) and (rightrecord[7] < d3max + 1):
                  dist = distanceham(int(leftrecord[4]), int(rightrecord[4]))
          if dist <= maxdiff:
            sql = sql + '(' + str(leftrecord[0]) + ',' + str(rightrecord[0]) + ',' + str(dist) + '),'
            nbfound = nbfound + 1

      if len(sql) > 60:
        fp.write(sql[:len(sql) - 1] + ';\n')
      if leftrecord[1] < maxdiff:     # Update 'done' flag except if already set with higher value
        sql = 'UPDATE timages set tsdupe=' + str(maxdiff) + ' WHERE Id=' + str(leftrecord[0]) + ';\n'
        fp.write(sql)

    durnosql = time.time() - t2
    leftrecord = leftrecords[0]
    s = "where d0=" + txtgreen + f"{leftrecord[5]}" + txtnocolor
    if optimdist3:
      s = s + f", d1={leftrecord[2]}, d2={leftrecord[6]}, d3 from {leftrecord[7]} to {lastrecord[7]}"
    else:
      if optimdist2:
        s = s + f", d1={leftrecord[2]}, d2 from {leftrecord[6]} to {lastrecord[6]}"
      else:
        s = s + f", d1 from {leftrecord[2]} to {lastrecord[2]}, d2 from {leftrecord[6]} to {lastrecord[6]}"
    current, peak = tracemalloc.get_traced_memory()
    # raminfo = f"Current memory usage {round(current / 1e6)} MB; Peak: {round(peak / 1e6)} MB"
    s = f"Thread: Comparison found{nbfound:4} results for " + txtgreen + f"{len(leftrecords):3}" + txtnocolor + f" X {len(locdata):6} records " + s + f", in " + \
        txtgreen + f"{duration(durnosql, False)}" + txtnocolor + f" for comparison and " + txtgreen + f"{duration(dursql,False)}" + txtnocolor + f" for SQL. RAM used {round(current / 1e6)} MB"
    # log(msg + '\n' + s, 3)
    log(s, 3)
    if (debug > 1) and (nbfound > 0): print(s)
    fp.close
    del locdata


def CompareSqlMain():
  global nbrecordscached, pool, watcher, jobs, jobcounter
  global records, maxleft, d0, scaninorder

  def loadrecords():
    global records, maxleft, d0

    cur = conn.cursor()
    records = []
    step = 0
    limrec = 500 * threads
    if d0 < 800: limrec = 1000 * threads
    if d0 > 900: limrec = 2000 * threads
    if scaninorder: limrec = 10 * limrec
    if maxdiff > 20: limrec = limrec // 10
    while (len(records) < limrec) and (d0 + step < 1729):       # 18 * 32 * 3 = 1728
      sql = 'SELECT Id, tsdupe, dist01, srcshortname, keyimg, dist00, dist02, dist03 FROM timages WHERE tsdupe<' + str(maxdiff) + ' AND dist00 = ' + str(d0 + step) + ' ORDER BY tsdupe, dist01, dist02, dist03;'
      log(sql, 3)
      cur.execute(sql)
      row = cur.fetchone()
      while (row is not None):
        records.append(row)
        row = cur.fetchone()
      step = step + 1
    if len(records) == 0: maxleft = 1728
    else: maxleft = records[len(records) - 1][5]
    log('Fetchall done. Returned ' + str(len(records)) + ' records. Dist00 range from ' + str(d0) + ' to ' + str(maxleft), 3)

  SQLexec("INSERT INTO tparam (paramkey, paramvalue) VALUES('threadid','0') ON CONFLICT (paramkey) DO UPDATE SET paramvalue = '0';", False, 2)
  log('Launch threads to compare each image from timages ordered by dist00 to others where:', 1)
  log('  dist00, dist01 and dist02 are in maxdiff range',1)
  log('  and, if scaninorder (ie no new images added since comparison start), dist00>=dist00',1)
  log('  and one of the 2 tsdupe<maxdiff (both >=maxdiff means already compared) ',1)
  log('  and not from the same video source ',1)
  log('Then compute distance between 2 images and if <=maxdiff insert into timagedupes,',1)
  log('  and set tsdupe=maxdiff (flag done)',1)
  log('Most images have a dist00 between 850 and 875. The duration is proportional to the image count power 1.5 (should be 2 but tend to 1 due to above optimizations), the RAM usage is proportionnal so you may have to use less threads.',1)
  log('',1)
  # SQLexec('CREATE INDEX IF NOT EXISTS idx_timages_tsdupe ON public.timages USING btree (tsdupe ASC NULLS LAST) TABLESPACE pg_default; ') # Index dropped on step 1 have to be created for step 2
  if speed < 3:
    sql = 'SELECT MIN(dist00), MAX(dist00) FROM timages WHERE tsdupe<' + str(maxdiff) + ';'
    log(sql, 1)
    cur.execute(sql)
    row = cur.fetchone()
    d0 = row[0]
    d0max = row[1]
    log('CompareSqlMain - Dist00 range from ' + str(d0) + ' to ' + str(d0max), 1)
  else:
    sql = "SELECT paramvalue FROM tparam WHERE paramkey='d0min';"
    log(sql, 3)
    cur.execute(sql)
    row = cur.fetchone()
    d0 = int(row[0])
    d0max = 1728

  # Test situation to see if optimization scan_in_order can be used (see thread code. 4 time less data to work on.)
  prevscaninorder = int(SQLexec("SELECT paramvalue FROM tparam WHERE paramkey='scaninorder';", True, 3))
  maxtsdupe = int(SQLexec("SELECT MAX(tsdupe) FROM timages;", True, 3))
  msg = ""
  scaninorder = False
  if (prevscaninorder <= maxtsdupe) and (maxdiff > maxtsdupe):
    scaninorder = True
    SQLexec("UPDATE tparam SET paramvalue='" + str(maxdiff) + "' WHERE paramkey='scaninorder';")
    conn.commit()
    msg = f"First comparison with maxdiff={maxdiff}. "
  if (prevscaninorder == maxtsdupe) and (maxdiff == maxtsdupe):
    scaninorder = True
    msg ="Continue precedent comparison. "
  if (prevscaninorder < 0) and (maxdiff == maxtsdupe):
    scaninorder = False
    msg ="Analyse was launched, new images to compare with previous ones. "
  if (prevscaninorder == maxtsdupe) and (maxdiff < maxtsdupe):
    scaninorder = False
    msg =f"Try to compare with lesser maxdiff than previous one. You can cancel this higher run with: UPDATE timages SET tsdupe={maxdiff} WHERE tsdupe>{maxdiff};"
  if msg != "":
    msg = f"prevscaninorder = {prevscaninorder}, maxtsdupe = {maxtsdupe}, maxdiff = {maxdiff}. " + txtgreen + f"scaninorder = {scaninorder}. " + txtnocolor + msg
    log(msg, 1)
  else:
    log(txtred + 'ERROR: ' + txtnocolor + f"prevscaninorder = {prevscaninorder}, maxtsdupe = {maxtsdupe}, maxdiff = {maxdiff}. Not in a planned case. Either a bug or an error in parameters. "
                                          f"You have to update tparam and tsdupe in timages to solve the situation.", 0)
    sys.exit(1)

  minright = d0
  maxleft = -1
  nbrecordscached = 0
  records = []
  finished = False

  if len(fnmatch.filter(os.listdir(folderimg), '*.sqltmp')) > 0:
    log('Load result of previous threads in SQL database', 1)
    for file in os.scandir(folderimg):
      if file.name[-7:] == '.sqltmp':
        # print('Rename ' + file.name + ' in ' + file.name[:-3])
        os.rename(folderimg + file.name, folderimg + file.name[:-3])
    for file in os.scandir(folderimg):
      if file.name[-4:] == '.sql':
        # log('Loading: ' + folderimg + file.name, 1)
        SQLscript(folderimg + file.name, 3)

  loadrecords()
  d0 = int(records[0][5])
  maxram = psutil.virtual_memory().percent

  while not(finished):
    SQLexec("INSERT INTO tparam (paramkey, paramvalue) VALUES('d0min','" + str(d0) + "') ON CONFLICT (paramkey) DO NOTHING;", False, 2)
    for file in os.scandir(folderimg):
      if file.name[-7:] == '.sqltmp':
        # print('Rename ' + file.name + ' in ' + file.name[:-3])
        os.rename(folderimg + file.name, folderimg + file.name[:-3])

    log("CompareSqlMain - Cached in RAM data with dist00 from " + txtgreen + f"{d0}" + txtnocolor + " to " + txtgreen + f"{maxleft}" + txtnocolor + f", ie {len(records):_d} records. ", 1)
    displaythreshold = 100

    if maxram > 95:
      nbthr = int(threads / maxram * 95)
      if nbthr < 1: nbthr = 1
      log(f"Due to RAM usage, create a pool of{txtgreen} {nbthr} {txtnocolor}threads. maxram={maxram}%")
    else:
      nbthr = threads
      if nbthr < 1: nbthr = 1
      log(f'Create a pool of{txtgreen} {nbthr} {txtnocolor}threads. maxram = {maxram}%')
    pool = multiprocessing.Pool(nbthr)
    # put listener to work first
    watcher = pool.apply_async(listener, args=(queue,), )
    jobs = []
    jobcounter = 0
    recblock = []
    prevts = -1
    prev00 = -1
    prev01 = -1
    prev02 = -1
    init02 = records[0][6]
    tstartthr = time.time()
    batchreccount = len(records)
    # When data density increase split in more blocks to benefit SQL optimizations, when it decrease group in bigger blocks to limit threading overhead. When blksize increase, RAM usage increase also linearly.
    # Change blksize parameter based on your RAM, CPU and dataset. Try to have max CPU but keep RAM<80% and SQL disk usage<80%
    # Scaninorder means 1st comparison and it uses 16 times less RAM due to optimisations (compare 1 images only to images with dist00,01,02 and 03>=current one)
    # A thread is called to compare <blksize> images and 1 query load the possible images to compare them: if too hi query contains non corresponding images, if too low there is too much SQL queries.
    comment = ""
    blksize02 = 3000000 / batchreccount
    if maxleft > d0 + 1:
      if scaninorder:
        blksize02 = blksize02 * ((maxleft - d0 + 1) ** 1.5)
      else:
        blksize02 = blksize02 * ((maxleft - d0 + 1))
    blksize02 = blksize02 * 90 / pow(maxdiff, 1.5);
    if maxram > 95:
      blksize02 = blksize02 / 2
    if blksize < 99:
      comment = f"Should be {blksize02} but -blksize set in command line. "
      blksize02 = min(blksize02, blksize)
    if blksize02 < 2: blksize02 = 1
    blksize02 = round(blksize02, 0)
    blksize01 = round(blksize02 / 5, 0)
    if blksize01 < 1: blksize01 = 0
    if scaninorder:
      comment = "Scaninorder. " + comment
    else:
      comment = "Not scaninorder. "
      blksize02 = 0
      blksize01 = 0

    log(f"dist00 from {txtgreen}{d0}{txtnocolor} to {txtgreen}{maxleft}{txtnocolor} => minimal blksize01 = {txtgreen}{blksize01}{txtnocolor}, blksize02 = {txtgreen}{blksize02}{txtnocolor}. maxram = {maxram}%. " + comment, 1)
    nbjobs = 0
    for leftrecord in records:
      ramusg = psutil.virtual_memory().percent
      if ramusg > maxram: maxram = ramusg
      rupture = False
      if ((len(recblock) > 0) and (leftrecord[5] != prev00)) or ((len(recblock) > blksize01) and (leftrecord[2] != prev01)) or ((len(recblock) > blksize02) and (leftrecord[6] != prev02)):
        log(f"Split in blocks if (({len(recblock)} > 0) and ({leftrecord[5]} != {prev00})) or (({len(recblock)} > {blksize01}) and ({leftrecord[2]} != {prev01})) or (({len(recblock)} > {blksize02}) and ({leftrecord[6]} != {prev02}))", 3)
      # if ((len(recblock) > 0) and ((leftrecord[2] != prev01) or (leftrecord[6] != prev02))):
        # In a single thread all dist00, dist01 AND dist02 and tsdupe are the same
        job = pool.apply_async(mpCompareThread, (recblock, queue))
        jobs.append(job)
        nbjobs = nbjobs + 1
        time.sleep(0.1)     # to avoid a crash with simultaneous requests (psycopg2.errors.InsufficientResources: too many dynamic shared memory segments)
        if not (queue.full):
          log('Execute 1 thread in the CompareSqlMain. Job queue len = ' + str(len(jobs)), 1)
          jobs[jobcounter].get()
          jobcounter = jobcounter + 1
        recblock = []
      recblock.append(leftrecord)
      prev00 = leftrecord[5]
      prev01 = leftrecord[2]
      prev02 = leftrecord[6]
    job = pool.apply_async(mpCompareThread, (recblock, queue))
    jobs.append(job)
    if not (queue.full):
      log('Execute 1 thread in the CompareSqlMain. Job queue len = ' + str(len(jobs)), 1)
      jobs[jobcounter].get()
      jobcounter = jobcounter + 1
    recblock = []

    log('Load result of previous threads in SQL database', 1)
    for file in os.scandir(folderimg):
      if file.name[-4:] == '.sql':
        # log('Loading: ' + folderimg + file.name, 1)
        SQLscript(folderimg + file.name, 3)

    d0 = maxleft + 1;
    loadrecords()
    if maxleft == d0max: finished = True

    while jobcounter < len(jobs):
      ramusg = psutil.virtual_memory().percent
      if ramusg > maxram: maxram = ramusg
      if (len(jobs) - jobcounter) % (threads * displaythreshold) == 0:
        raminfo = f"cpu: {psutil.cpu_percent(0.1)}%, RAM: {psutil.virtual_memory().percent}%, available {round(psutil.virtual_memory().available/1073741824)} Gb / {round(psutil.virtual_memory().total/1073741824)} Gb"
        log(f"Wait for {(len(jobs) - jobcounter):4} jobs to finish. " + raminfo, 1)

      jobs[jobcounter].get()
      jobcounter = jobcounter + 1
    log(f"End of CompareSqlMain loop.{txtgreen} {duration(time.time() - tstartthr, False)} {txtnocolor}for {batchreccount} records finished. Changing memory cache.", 1)
    queue.put('kill')
    pool.close()

  log(f"Compare ended", 1)
  SQLexec("UPDATE tparam SET paramvalue='" + str(maxdiff) + "' WHERE paramkey='scaninorder';")


def mpUnwantedThread(uw, queue):
  global maxdiffuw

  mpperf = time.time()
  exiterr = False
  mpconn = None
  try:
    params = config()
    mpconn = psycopg2.connect(**params)
    mpcur = mpconn.cursor()
  except (Exception, psycopg2.DatabaseError) as error:
    exiterr = True
    print(error)

  if not(exiterr):
    #uw = keyimg, dist00, dist01, dist02, dist03
    sql = 'SELECT I.keyimg, I.dist00, I.dist01, I.Id, S.imgfld, I.srcshortname, I.imgshortname FROM timages I, tsources S WHERE I.srcshortname=S.srcshortname ' \
          'AND I.dist00 >= ' + str(uw[1]-maxdiffuw) + ' AND I.dist00 <= ' + str(uw[1]+maxdiffuw) + ' AND I.dist01 >= ' + str(uw[2]-maxdiffuw) + ' AND I.dist01 <= ' + str(uw[2]+maxdiffuw) + \
          'AND I.dist02 >= ' + str(uw[3]-maxdiffuw) + ' AND I.dist02 <= ' + str(uw[3]+maxdiffuw) + 'AND I.dist03 >= ' + str(uw[4]-maxdiffuw) + ' AND I.dist03 <= ' + str(uw[4]+maxdiffuw) + ';'
    mpcur.execute(sql)
    log('mpUnwantedThread: ' + str(mpcur.rowcount) + ' records with SQL: ' + sql,3)
    locimages = mpcur.fetchall()

    # print(len(locimages))
    nbfound = 0
    fp = open(folderimg + 'lid_' + str(os.getpid()) + '.sqltmp', 'a')
    for image in locimages:                            # I.keyimg, I.dist00, I.dist01, I.Id, S.imgfld, I.srcshortname, I.imgshortname
      dist = distanceham(int(image[0]), int(uw[0]))
      log(f"mpUnwantedThread: abs({image[1]} - {uw[1]}) <= {maxdiffuw}) and (abs({image[2]} - {uw[2]}) <= {maxdiffuw} and dist = {dist}", 3)
      if dist <= maxdiffuw:
        log('mpUnwantedThread: dist00 and dist01 small enough. dist=' + str(dist) + ', Id=' + str(image[3]), 3)
        fp.write('DELETE FROM timages WHERE Id=' + str(image[3]) + ';\n')
        fn = image[4] + image[5] + '/' + image[6]
        if os.path.isfile(fn):
          log('mpUnwantedThread: Remove ' + fn, 3)
          try:
            os.remove(fn)
          except:
            log('mpUnwantedThread: Error removing ' + fn, 3)
        nbfound = nbfound + 1

    if nbfound > 0:
      dur = time.time() - mpperf
      log('mpUnwantedThread: Unwanted images found ' + f"{nbfound: }" + ' in ' + duration(dur, False), 2)
    fp.close
    result = nbfound


def UnwantedMain():
  global conn, cur, pool, watcher, jobs, jobcounter, smm

  log('Compute key-images for unwanted files in ' + folderuw, 1)
  uwpack = []
  newuw = False
  for file in os.scandir(folderuw):
    if file.name[-4:] == '.jpg':
      fn = file.name
      key = calcfp(folderuw, fn)
      uwpack.append([fn, key, distanceham(key, ckey00), distanceham(key, ckey01), distanceham(key, ckey02), distanceham(key, ckey03)])
      os.remove(folderuw + fn)
      newuw = True
      if len(uwpack) % 1000 == 0:
        # current, peak = tracemalloc.get_traced_memory()
        # raminfo = f"Current memory usage {round(current / 1e6)} MB; Peak: {round(peak / 1e6)} MB"
        sql = ",".join("('%s', '%s', %s, %s, %s, %s)" % (a, b, c, d, e, f) for (a, b, c, d, e, f) in uwpack)
        sql = 'INSERT INTO timagesuw (imgshortname, keyimg, dist00, dist01, dist02, dist03) VALUES ' + sql + ';'
        SQLexec(sql, False, 3)
        conn.commit()
        uwpack = []
        log(f"1000 done yet... ", 1)
    else:
      if file.name[-4:] != '.txt':
        log('In ' + folderuw + ' the file ' + file.name + ' is not a jpg: ' + file.name[-4:])
  if len(uwpack) > 0:
    sql = ",".join("('%s', '%s', %s, %s, %s, %s)" % (a, b, c, d, e, f) for (a, b, c, d, e, f) in uwpack)
    sql = 'INSERT INTO timagesuw (imgshortname, keyimg, dist00, dist01, dist02, dist03) VALUES ' + sql + ';'
    SQLexec(sql, False, 3)
    conn.commit()
  if newuw:
    SQLexec('SELECT COUNT(*) FROM timagesuw;', True)
    SQLexec('CREATE TABLE IF NOT EXISTS timagesuwtmp (Id SERIAL NOT NULL PRIMARY KEY, imgshortname TEXT, keyimg TEXT, dist00 INTEGER, dist01 INTEGER, dist02 INTEGER, dist03 INTEGER);')
    SQLexec('INSERT INTO timagesuwtmp (imgshortname, keyimg, dist00, dist01, dist02, dist03) SELECT MIN(imgshortname), keyimg, MIN(dist00), MIN(dist01), MIN(dist02), MIN(dist03) FROM timagesuw GROUP BY keyimg;')
    SQLexec('DROP TABLE timagesuw;')
    SQLexec('ALTER TABLE timagesuwtmp RENAME TO timagesuw;')
    SQLexec('SELECT COUNT(*) FROM timagesuw;', True)
    conn.commit()
  else:
    log('No new unwant image added in timagesuw', 1)

  if (len(uwpack) > 0) or (speed == 1):
    cur.execute('SELECT keyimg, dist00, dist01, dist02, dist03 FROM timagesuw ORDER BY dist00, dist01, dist02, dist03;')
    uws = cur.fetchall()
    # current, peak = tracemalloc.get_traced_memory()
    # raminfo = f"Current memory usage {round(current / 1e6)} MB; Peak: {round(peak / 1e6)} MB"
    nt = threads
    log(f"Start {nt} threads to compare all images vs unwanted keys. Delete images with distance from a unwanted < {maxdiffuw}. ", 1)
    pool = multiprocessing.Pool(nt)
    watcher = pool.apply_async(listener, args=(queue,), )
    jobs = []
    jobcounter = 0

    for uw in uws:
      job = pool.apply_async(mpUnwantedThread, (uw, queue))
      jobs.append(job)
      if not (queue.full):
        log('Execute 1 thread in the CompareSqlMain. Job queue len = ' + str(len(jobs)), 2)
        jobs[jobcounter].get()
        jobcounter = jobcounter + 1

    while jobcounter < len(jobs):
      if (len(jobs) - jobcounter) % (threads * 100) == 0:
        # current, peak = tracemalloc.get_traced_memory()
        # raminfo = f"Current memory usage {round(current / 1e6)} MB; Peak: {round(peak / 1e6)} MB"
        log(f"Wait for {(len(jobs) - jobcounter):_d} jobs to finish", 1)
      jobs[jobcounter].get()
      jobcounter = jobcounter + 1
    pool.close()
    queue.put('kill')
    log('Load result of threads in SQL database', 1)
    for file in os.scandir(folderimg):
      if file.name[-7:] == '.sqltmp':
        # print('Rename ' + file.name + ' in ' + file.name[:-3])
        os.rename(folderimg + file.name, folderimg + file.name[:-3])
    for file in os.scandir(folderimg):
      if file.name[-4:] == '.sql':
        # log('Loading: ' + folderimg + file.name, 1)
        SQLscript(folderimg + file.name, 2)
    log('Remove unwanted images from timages done.', 1)
    SQLexec('SELECT COUNT(*) FROM timages;', True)


def UnwantedPairs():
  fn = ''
  for file in os.scandir(folderuw):
    fn = file.name
    if fn[-4:] == '.txt':
      fp = open(file, 'r')
      lgn = fp.readline()
      if lgn[:10] == 'To move in':
        srcshortname1 = fp.readline()
        srcshortname1 = srcshortname1[:-1]
        srcshortname2 = fp.readline()
        srcshortname2 = srcshortname2[:-1]
        if srcshortname1 < srcshortname2:
          sql = "INSERT INTO tpairsuw (srcshortname1, srcshortname2) VALUES ('" + srcshortname1 + "', '" + srcshortname2 + "');"
        else:
          sql = "INSERT INTO tpairsuw (srcshortname1, srcshortname2) VALUES ('" + srcshortname2 + "', '" + srcshortname1 + "');"
        SQLexec(sql)
        # sql = "DELETE FROM timagedupes D USING timages I1, timages I2 WHERE D.idimage1=I1.id AND D.idimage2=I2.id AND I1.srcshortname='" + srcshortname1 + "' AND I2.srcshortname='" + srcshortname2 + "';"
        # SQLexec(sql)
        # sql = "DELETE FROM timagedupes D USING timages I1, timages I2 WHERE D.idimage1=I1.id AND D.idimage2=I2.id AND I2.srcshortname='" + srcshortname1 + "' AND I1.srcshortname='" + srcshortname2 + "';"
        # SQLexec(sql)
      else:
        log(txtred + 'ERROR: ' + txtnocolor + 'Bad structure of file ' + fn + ': lgn[:10] = ' + lgn[:10], 1)
      fp.close()
      # log(f"os.remove({folderuw} + {fn})", 3)
      # os.remove(folderuw + fn)
  if fn != '':
    SQLexec('SELECT COUNT(*) FROM tpairsuw;', True)
    SQLexec('CREATE TABLE IF NOT EXISTS tpairsuwtmp (Id SERIAL NOT NULL PRIMARY KEY, srcshortname1 TEXT, srcshortname2 TEXT);')
    SQLexec('INSERT INTO tpairsuwtmp (srcshortname1, srcshortname2) SELECT DISTINCT srcshortname1, srcshortname2 FROM tpairsuw;')
    SQLexec('DROP TABLE tpairsuw;')
    SQLexec('ALTER TABLE tpairsuwtmp RENAME TO tpairsuw;')
    SQLexec('SELECT COUNT(*) FROM tpairsuw;', True)
    conn.commit()


def SQLclean():
  global conn, cur

  log('Load result of previous threads in SQL database', 1)
  for file in os.scandir(folderimg):
    if file.name[-7:] == '.sqltmp':
      # print('Rename ' + file.name + ' in ' + file.name[:-3])
      os.rename(folderimg + file.name, folderimg + file.name[:-3])
  for file in os.scandir(folderimg):
    if file.name[-4:] == '.sql':
      # log('Loading: ' + folderimg + file.name, 1)
      SQLscript(folderimg + file.name, 2)

  if (speed < 3) and not(sameresult):
    log('************************************************************************', 1)
    # log('*               PRESS ESC TO ABORT (skipped with speed=3)              *', 0)
    # log('************************************************************************', 1)
    if speed == 1 or step2 or step3:
      log('', 0)
      log(txtgreen + 'Clean database of duplicates and obsolete in timages' + txtnocolor, 0)
      SQLexec('SELECT COUNT(*) FROM timages;', True)
      SQLexec('DELETE FROM timages WHERE Id IN (SELECT MIN(id) FROM timages GROUP BY srcshortname, imgshortname HAVING count(*) > 1);')
      conn.commit()
      SQLexec("DELETE FROM timages WHERE id IN (SELECT timages.Id FROM timages LEFT JOIN tsources ON timages.srcshortname=tsources.srcshortname WHERE tsources.srcshortname IS NULL);")
      conn.commit()
      SQLexec('SELECT COUNT(*) FROM timages;', True)

    if speed == 1 or step3:
      SQLexec('SELECT COUNT(*) FROM timagedupes;', True)
      SQLexec('CREATE TABLE timagedupestmp (idimage1 INTEGER, idimage2 INTEGER, dist INTEGER);')
      SQLexec('INSERT INTO timagedupestmp (idimage1, idimage2, dist) SELECT idimage1, idimage2, min(dist) FROM timagedupes ti, timages i1, timages i2 '
              'WHERE ti.idimage1=i1.id AND ti.idimage2=i2.id GROUP BY idimage1, idimage2')
      SQLexec('DROP TABLE timagedupes;')
      SQLexec('ALTER TABLE timagedupestmp RENAME TO timagedupes;')
      conn.commit()
      SQLexec('CREATE INDEX IF NOT EXISTS idx_timagedupes_id1 ON public.timagedupes USING btree (idimage1 ASC NULLS LAST) TABLESPACE pg_default;')
      SQLexec('CREATE INDEX IF NOT EXISTS idx_timagedupes_id2 ON public.timagedupes USING btree (idimage2 ASC NULLS LAST) TABLESPACE pg_default;')
      SQLexec('SELECT COUNT(*) FROM timagedupes;', True)


def SQLanalyse():
  global conn, cur

  def testncopy(src, dst, com):
    if os.path.exists(src):
      log('Copy ' + txtgreen + com + txtnocolor + src + ' ' + dst, 3)
      shutil.copy2(src, dst)
      return True
    else:
      log(txtred + 'ERROR: ' + txtgreen + com + txtnocolor + src + ' not exists.', 1)
      return False

  if sameresult:
    log('-sameresult set: do nothing in SQL and do the file copy', 1)
  else:
    if not (StopAsked()):
      log('', 0)
      # log('***********************************************', 0)
      # log('', 0)
      log('Create, populate and remove duplicate of pair of images with source and image names, limiting to specific distance', 0)
      SQLexec('SELECT COUNT(*) FROM tmpimgpairs;', True)
      SQLexec('DROP TABLE IF EXISTS tmpimgpairs;')
    if not (StopAsked()):
      SQLexec('CREATE TABLE tmpimgpairs (dist INTEGER, Lid INTEGER, Lsrcshortname TEXT, Limgshortname TEXT, Rid INTEGER, Rsrcshortname TEXT, Rimgshortname TEXT, IdSrcPair INTEGER);')
      SQLexec('INSERT INTO tmpimgpairs (dist, LId, Lsrcshortname, Limgshortname, RId, Rsrcshortname, Rimgshortname) ' \
                'SELECT DISTINCT D.dist, L.Id, L.srcshortname, L.imgshortname, R.Id, R.srcshortname, R.imgshortname ' \
                'FROM timagedupes as D ' \
                'INNER JOIN timages as L ON D.idimage1=L.Id ' \
                'INNER JOIN timages as R ON D.idimage2=R.Id ' \
                'WHERE L.srcshortname < R.srcshortname AND dist<=' + str(maxdiff) + ' LIMIT 50000000;')
      conn.commit()
    if not (StopAsked()):
      if SQLexec('SELECT COUNT(*) FROM tmpimgpairs;', True) > 10000000:
        log(txtred + 'WARNING: ' + txtnocolor + 'Partial result. Use result to remove images, select more restrictive parameters and relaunch.', 0)
      else:
        SQLexec('INSERT INTO tmpimgpairs (dist, LId, Lsrcshortname, Limgshortname, RId, Rsrcshortname, Rimgshortname) ' \
                  'SELECT DISTINCT D.dist, R.Id, R.srcshortname, R.imgshortname, L.Id, L.srcshortname, L.imgshortname ' \
                  'FROM timagedupes as D ' \
                  'INNER JOIN timages as L ON D.idimage1=L.Id ' \
                  'INNER JOIN timages as R ON D.idimage2=R.Id ' \
                  'WHERE L.srcshortname > R.srcshortname AND dist<=' + str(maxdiff) + ';')
        conn.commit()
    if not (StopAsked()):
      SQLexec('CREATE INDEX idx_tmpimgpairs ON tmpimgpairs (Lsrcshortname, Rsrcshortname) TABLESPACE pg_default;')
      conn.commit()
      SQLexec('SELECT COUNT(*) FROM tmpimgpairs;', True)
      SQLexec('DELETE FROM tmpimgpairs T USING tpairsuw P WHERE T.Lsrcshortname = P.srcshortname1 AND T.Rsrcshortname = P.srcshortname2;')
    if not (StopAsked()):
      SQLexec('SELECT COUNT(*) FROM tmpimgpairs;', True)

    if not (StopAsked()):
      log('', 0)
      # log('***********************************************', 0)
      # log('', 0)
      log('Create a table to reference pairs of source with an Id', 0)
      SQLexec('SELECT COUNT(*) FROM tmppairsofsources;', True)
      SQLexec('DROP TABLE IF EXISTS tmppairsofsources;')
    if not (StopAsked()):
      SQLexec('CREATE TABLE tmppairsofsources (iddupepair SERIAL NOT NULL PRIMARY KEY UNIQUE, Lsrcshortname TEXT, Rsrcshortname TEXT, nb INTEGER);')
      SQLexec('INSERT INTO tmppairsofsources (Lsrcshortname, Rsrcshortname, nb) ' \
                'SELECT Lsrcshortname, Rsrcshortname, COUNT(*) FROM tmpimgpairs GROUP BY Lsrcshortname, Rsrcshortname HAVING COUNT(*) >= ' + str(minimg) + ';')
      conn.commit()
    if not (StopAsked()):
      SQLexec('CREATE INDEX idx_tmppairsofsources ON tmppairsofsources (Lsrcshortname, Rsrcshortname);')
      SQLexec('SELECT COUNT(*) FROM tmppairsofsources;', True)

    if not (StopAsked()):
      log('', 0)
      log('Update tmpimgpairs with the reference of source pairs', 0)
    if not (StopAsked()):
      SQLexec('SELECT COUNT(*) FROM tmpimgpairs;', True)
      SQLexec('UPDATE tmpimgpairs i SET IdSrcPair=s.iddupepair FROM tmppairsofsources s WHERE i.Lsrcshortname=s.Lsrcshortname AND i.Rsrcshortname=s.Rsrcshortname;')
      conn.commit()
      SQLexec('DELETE FROM tmpimgpairs WHERE idsrcpair IS NULL;')
      conn.commit()
      SQLexec('SELECT COUNT(*) FROM tmpimgpairs;', True)
    if not (StopAsked()):
      SQLexec('SELECT COUNT(*) FROM tmpresult;', True)
      SQLexec('DROP TABLE IF EXISTS tmpresult;')
      SQLexec('CREATE TABLE tmpresult (idsrcpair INTEGER, srcshortname TEXT, imgshortname TEXT, Id INTEGER);')
      SQLexec('INSERT INTO tmpresult (idsrcpair, srcshortname, imgshortname, Id) SELECT DISTINCT idsrcpair, Lsrcshortname, Limgshortname, Lid FROM tmpimgpairs;')
      SQLexec('INSERT INTO tmpresult (idsrcpair, srcshortname, imgshortname, Id) SELECT DISTINCT idsrcpair, Rsrcshortname, Rimgshortname, Rid FROM tmpimgpairs;')
      conn.commit()
      SQLexec('DELETE FROM tmpresult WHERE idsrcpair IN '\
              '(SELECT DISTINCT idsrcpair FROM (SELECT idsrcpair, srcshortname, COUNT(*) AS nb FROM tmpresult GROUP BY srcshortname, idsrcpair HAVING COUNT(*)<' + str(minimg) + ') AS A);')
      SQLexec('SELECT COUNT(*) FROM tmpresult;', True)

  if not (StopAsked()):
    sql = 'SELECT idsrcpair, tmpresult.srcshortname AS srcshortname, imgshortname, srcfld, imgfld, Id FROM tmpresult JOIN tsources ON tmpresult.srcshortname=tsources.srcshortname ORDER BY idsrcpair, tmpresult.srcshortname;'
    log(sql, 1)
    cur.execute(sql)
    records = cur.fetchall()
    if os.path.isdir(folderrs + 'dupesrc'):
      shutil.rmtree(folderrs + 'dupesrc')
    if os.path.isdir(folderrs + 'dupesrc'):
      log(txtred + 'ERROR: ' + txtnocolor + 'Folder removal of ' + folderrs + 'dupesrc' + ' did not worked.')
      halt
    os.mkdir(folderrs + 'dupesrc')
    log('Found: ' + str(len(records)) + ' files to copy in ' + folderrs + 'dupesrc', 1)
    previd = [[]]
    srclst = []
    nbcandidate = 0
    if len(records) > 0:
      for row in records:
        if row[0] != previd[0]:   # New set of duplicate encountered
          if len(srclst) == 2:        # Finish previous set of duplicate
            fp = open(folderrs + 'dupesrc/' + str(previd[0]) + '/nb_match.' + srclst[0] + '.' + srclst[1] + '.txt', 'w')
            fp.write('To move in ' + folderuw + ' to remove this pair from future comparison :\n')
            fp.write(srclst[0] + '\n')
            fp.write(srclst[1] + '\n')
            fp.write('#\n')
            sql = "SELECT dist, Lsrcshortname, Limgshortname, Rsrcshortname, Rimgshortname FROM tmpimgpairs WHERE Lsrcshortname='" + srclst[0] + "' AND Rsrcshortname='" + srclst[1] + "';"
            cur.execute(sql)
            impairs = cur.fetchall()
            for ip in impairs:
              fp.write(str(ip[0]) + ' between ' + ip[1] + '/' + ip[2] + ' and ' + ip[3] + '/' + ip[4] + '\n')
            fp.close()
          log('os.mkdir(' + folderrs + 'dupesrc/' + str(row[0]), 3)
          os.mkdir(folderrs + 'dupesrc/' + str(row[0]))
          if (row != records[0]) and (not((cptsrc > 1) or nosrccp) or (cptimg < 2*minimg)):
            log(f"Candidate failed to respect conditions: cptsrc={cptsrc}, ctpimg={cptimg}. Removing dupesrc/" + str(previd[0]), 1)
            shutil.rmtree(folderrs + 'dupesrc/' + str(previd[0]))
          cptsrc = 0
          cptimg = 0
          nbcandidate = nbcandidate + 1
          previd = row
          prevsrc = ''
          srclst = []
        if testncopy(row[4] + row[1] + '/' + row[2], folderrs + 'dupesrc/' + str(row[0]) + '/' + row[1] + '.' + row[2], 'image '):
          cptimg = cptimg + 1
        else:
          # Image file not found: remove from timages table
          sql = "DELETE FROM timages WHERE Id=" + str(row[5]) +";"
          log(sql, 1)
          cur.execute(sql)
        if prevsrc != row[1]:
          if not (nosrccp):
            if testncopy(row[3] + row[1], folderrs + 'dupesrc/' + str(row[0]) + '/' + pathtoshort(row[3] + row[1], foldervideo), 'source '):
              cptsrc = cptsrc + 1
          prevsrc = row[1]
          srclst.append(row[1])
      if len(srclst) == 2:
        # print(srclst)
        fname = folderrs + 'dupesrc/' + str(previd[0]) + '/nb_match.' + srclst[0] + '.' + srclst[1] + '.txt'
        # print(fname)
        fp = open(fname, 'w')
        fp.write('To move in ' + folderuw + ' to remove this pair from future comparison :\n')
        fp.write(srclst[0] + '\n')
        fp.write(srclst[1] + '\n')
        fp.write('#\n')
        sql = "SELECT dist, Lsrcshortname, Limgshortname, Rsrcshortname, Rimgshortname FROM tmpimgpairs WHERE Lsrcshortname='" + srclst[0] + "' AND Rsrcshortname='" + srclst[1] + "';"
        cur.execute(sql)
        impairs = cur.fetchall()
        for ip in impairs:
          fp.write(str(ip[0]) + ' between ' + ip[1] + '/' + ip[2] + ' and ' + ip[3] + '/' + ip[4] + '\n')
        fp.close()
      if not((cptsrc > 1) or nosrccp) or (cptimg < 2 * minimg):
        log(f"Candidate failed to respect conditions: cptsrc={cptsrc}, ctpimg={cptimg}. Removing dupesrc/" + str(previd[0]), 1)
        shutil.rmtree(folderrs + 'dupesrc/' + str(previd[0]))
    log('Copied ' + str(nbcandidate) + ' candidates into ' + folderrs + 'dupesrc', 0)

  if not (StopAsked()) and (speed == 1):
    log(txtgreen + 'Multiple repetitions of images with dist=0 (same keyimg)' + txtnocolor, 0)
    os.mkdir(folderrs + 'multiplerepetitions')
    SQLexec('CREATE INDEX IF NOT EXISTS idx_timages_keyimg ON public.timages USING btree (keyimg, srcshortname) TABLESPACE pg_default; ') # Index dropped on step 1 have to recreated on step 3
    if not sameresult:
      SQLexec('DROP TABLE IF EXISTS tmprepetedkey;')
      SQLexec('CREATE TABLE tmprepetedkey (keyimg TEXT, repeted INTEGER);')
      # SQLexec('INSERT INTO tmprepetedkey (keyimg, repeted) SELECT keyimg, COUNT(*) FROM timages GROUP BY keyimg HAVING COUNT(*) > 2;')
      SQLexec('INSERT INTO tmprepetedkey (keyimg, repeted) SELECT keyimg, COUNT(*) FROM (SELECT DISTINCT keyimg, srcshortname FROM timages) AS T GROUP BY keyimg HAVING COUNT(*) > 2;')
    SQLexec('CREATE INDEX IF NOT EXISTS idx_tmprepetedkey_keyimg ON public.tmprepetedkey USING btree (keyimg ASC NULLS LAST) TABLESPACE pg_default; ')
    SQLexec('DROP TABLE IF EXISTS tmp;')
    SQLexec('CREATE TABLE tmp (imgfld TEXT, srcshortname TEXT, imgshortname TEXT, repeted INTEGER, keyimg TEXT);')
    SQLexec('INSERT INTO tmp (imgfld, srcshortname, imgshortname, repeted, keyimg) SELECT S.imgfld, S.srcshortname, K.imgshortname, K.repeted, K.keyimg FROM ' \
          '(SELECT imgshortname, srcshortname, repeted, R.keyimg FROM timages I JOIN tmprepetedkey R ON I.keyimg=R.keyimg) K ' \
          'JOIN tsources S ON K.srcshortname=S.srcshortname ORDER BY K.keyimg, K.imgshortname;')
    sql = 'SELECT keyimg, COUNT(*) FROM (SELECT DISTINCT keyimg, srcshortname FROM tmp) A GROUP BY keyimg HAVING COUNT(*) > 2;'

    log(sql, 1)
    cur.execute(sql)
    keys = cur.fetchall()
    cptf = 0
    nbres = 0
    os.mkdir(folderrs + 'multiplerepetitions/0')
    for key in keys:
      cur.execute("SELECT imgfld, srcshortname, MIN(imgshortname), MIN(repeted), keyimg FROM tmp WHERE keyimg='" + key[0] + "' GROUP BY imgfld, srcshortname, keyimg;")
      records = cur.fetchall()
      distinctsources = 0
      prev = ''
      for row in records:
        if row[0] != prev:
          distinctsources = distinctsources + 1
          prev = row[0]
      if distinctsources > 2:
        nbres = nbres + 1
        if nbres > 1000:
          cptf = cptf + 1
          nbres = 0
          os.mkdir(folderrs + 'multiplerepetitions/' + str(cptf))
        for row in records:
          testncopy(row[0] + row[1] + '/' + row[2], folderrs + 'multiplerepetitions/' + str(cptf) + '/' + row[1] + '.' + row[2], 'image ')
    log('Copied ' + str(len(records)) + ' candidates into ' + folderrs, 0)


def helpprt():
  log('************************************************************************************', 0)
  log('Video DeDup : find video duplicates', 0)
  log('Copyright (C) 2021  Pierre Crette ' + version, 0)
  log('', 0)
  log('This program is free software: you can redistribute it and/or modify', copyright)
  log('it under the terms of the GNU General Public License as published by', copyright)
  log('the Free Software Foundation, either version 3 of the License, or', copyright)
  log('(at your option) any later version.', copyright)
  log('', copyright)
  log('This program is distributed in the hope that it will be useful,', copyright)
  log('but WITHOUT ANY WARRANTY; without even the implied warranty of', copyright)
  log('MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the', copyright)
  log('GNU General Public License for more details.', copyright)
  log('', copyright)
  log('You should have received a copy of the GNU General Public License', copyright)
  log('along with this program.  If not, see <http://www.gnu.org/licenses/>.', copyright)
  log('', copyright)
  log('SYNTAX 1: 1parse -step=0 image1.jpg [image2.jpg...]', copyright)
  log('SYNTAX 2: 1parse [options]', copyright)
  log('-v=n           Verbose mode', copyright)
  log('-step=n        1 to parse sources, 2 to compare images, 3 to analyse result or a combinaison. Default = 123', copyright)
  log('-tmp=          Working temporary folder. /tmp/ by default. A Ram drive of 64Mb can improve performance.', copyright)
  log('-sqldb=        SQLite database. If not exists will be created.', copyright)
  log('-sqlbak=       Backup where database is copied from time to time', copyright)
  log('-speed=2       Default=2. 1: force aditional actions and remove unreferenced images. 3: skip some operations (faster succesive runs)', copyright)
  log('-p             Parallel. Will set lock files (*.run) to avoid 2 computers to work on the same source. Between 2 runs execute: find . -name *.run -exec rm {} \;', copyright)
  log('-threads=n     Number of threads to use.', copyright)
  log('-log=file      Log file', copyright)
  log('', copyright)
  log('STEP 1: ANALYSE', copyright)
  log('  Speed=3 to consider sources with not yet any image; Speed=2 to consider all sources; Speed=1 add a long control of jpeg images count before creating new ones.', copyright)
  log('-fps=n         fps: take 1 picture each n seconds. Default fps=1/60 ie 1 picture every 60 seconds.', copyright)
  log('-limffmpeg=n   Percentage of image created by ffmpeg to consider enough. Default=99', copyright)
  log('-noffmpeg      Skip ffmpeg. Useful when some cannot achieve the 99% image creation', 0)
  log('-delparam      Delete param.txt if number of jpeg images < limffmpeg set. Works only if speed=1', 0)
  log('', copyright)
  log('STEP 2: COMPARE', copyright)
  log('  Speed=2 add SQL cleanup before comparison; Speed=1 remove unwanted before.', copyright)
  log('-maxdiff=n     Difference between 2 images to consider them similar. Distance = number of bits differents in a 32x18x3 key. Range from 0 to 1728.', copyright)
  log('-blksize=n     Technical parameter for comparison cache. No functional impact. RAM usage impact. Use this to override dynamic value.', copyright)
  log('', copyright)
  log('STEP 3: ANALYSE', copyright)
  log('  Speed=1 add a last step of finding exact repetition in 3 or more sources.', copyright)
  log('-minimg=n      (*) Minimum number of similar images to declare a pair of source as duplicate.', copyright)
  log('-uw=folder     Unwanted images and sourceduplicates folder. default is folderimage/unwanted. Jpeg will be erased, use copies!', copyright)
  log('-maxdiffuw=n   Max distance between an image and an unwanted to remove the image (sql and jpeg). Default=0', copyright)
  log('-rs=folder     Resultset folder where candidate duplicate will be copied for manual review.', copyright)
  log('-sameresult    Keep tmpResult table untouched from previous run including previous maxdiff parameter.', copyright)
  log('-nosrccp       No sources copy. Whith t=1 to find useless images.', copyright)
  log('************************************************************************************', 0)
  log('', copyright)


if __name__ == '__main__':
  # Step0: Read arguments and initialize variables
  # set_start_method("spawn")

  tracemalloc.start()
  flog = open(logfile, 'w')
  print('')
  # print (str(sys.argv))
  perf = time.perf_counter()
  conn = None
  try:
    params = config()
    conn = psycopg2.connect(**params)
    cur = conn.cursor()
    cur.execute('SELECT version()')
    db_version = cur.fetchone()
    log('PostgreSQL database version: ' + str(db_version), 1)
    # print(db_version)
  except (Exception, psycopg2.DatabaseError) as error:
    print(error)

  tmp = '/tmp/' + pid + '/'
  print(sys.argv)
  for i in sys.argv[1:]:
    log('arg: ' + i, 2)
    if i[:5] == '-src=':
      foldervideo = i[5:]
      if foldervideo[-1] != '/': foldervideo = foldervideo + '/'
      i = ''
    if i[:5] == '-img=':
      folderimg = i[5:]
      i = ''
    if i[:3] == '-v=':
      debug = int(i[3:])
      i = ''
    if i == '-nc':
      copyright = 12
      i = ''
    if i[:5] == '-fps=':
      fpsn = float(i[5:])
      i = ''
    if i[:9] == '-threads=':
      threads = int(i[9:])
      i = ''
    if i[:5] == '-tmp=':
      if i[-1] == '/':
        tmp = i[5:] + pid + '/'
      else:
        tmp = i[5:] + '/' + pid + '/'
      i = ''
    if i[:7] == '-sqldb=':
      sqldb = i[7:]
      i = ''
    if i[:8] == '-sqlbak=':
      sqlbak = i[8:]
      i = ''
    if i[:4] == '-rs=':
      folderrs = i[4:]
      i = ''
    if i[:4] == '-uw=':
      folderuw = i[4:]
      i = ''
    if i == '-p':
      parallel = True
      i = ''
    if i[:7] == '-speed=':
      speed = int(i[7:])
      i = ''
    if i[:9] == '-blksize=':
      blksize = int(i[9:])
      i = ''
    if i[:11] == '-limffmpeg=':
      limffmpeg = int(i[11:])
      i = ''
    if i[:9] == '-maxdiff=':
      maxdiff = int(i[9:])
      i = ''
    if i[:11] == '-maxdiffuw=':
      maxdiffuw = int(i[11:])
      i = ''
    if i[:8] == '-minimg=':
      minimg = int(i[8:])
      i = ''
    if i[:6] == '-step=':
      for j in i[6:]:
        if j == '0':
          step0 = True
          listimages = []
        if j == '1': step1 = True
        if j == '2': step2 = True
        if j == '3': step3 = True
      i = ''
    if i == '-noffmpeg':
      noffmpeg = True
      i = ''
    if i == '-delparam':
      delparam = True
      i = ''
    if i == '-sameresult':
      sameresult = True
      i = ''
    if i == '-nosrccp':
      nosrccp = True
      i = ''
    if i != '':
      if step0:
        listimages.append(i)
      else:
        log('ERROR parameter ' + i + ' not valid !', 0)
        sys.exit(1)

  if foldervideo[-1] != '/': foldervideo = foldervideo + '/'
  if folderimg[-1] != '/': folderimg = folderimg + '/'
  if folderuw[-1] != '/': folderuw = folderuw + '/'
  if folderrs[-1] != '/': folderrs = folderrs + '/'
  if sqldb == '': sqldb = folderimg + 'VideoDedup.db'
  if not (os.path.exists(foldervideo)):
    log(txtred + 'Error: ' + foldervideo + ' does not exists. Use -src= parameter' + txtnocolor, 0)
    sys.exit()
  if not (os.path.exists(folderimg)): os.makedirs(folderimg)
  if not (os.path.exists('log')): os.makedirs('log')
  if not (os.path.exists(tmp)): os.makedirs(tmp)
  if not (os.path.exists(tmp)):
    log('ERROR Creation of tmp folder failed', 0)
    sys.exit(1)

  helpprt()
  log('nb args : ' + str(len(sys.argv) - 1), 5)
  log('abspath' + os.path.abspath(foldervideo + '..'), 5)
  log('basename' + os.path.basename(foldervideo), 5)
  log('dirname' + os.path.dirname(foldervideo), 5)
  log('sqldb       = ' + sqldb, copyright)
  log('tmp         = ' + tmp, copyright)
  log('debug       = ' + str(debug), copyright)
  log('threads     = ' + str(threads), copyright)
  if parallel: log(txtgreen + 'PARALLEL MODE : ' + txtnocolor + 'You can use other instances on other terminals or other computer. NONE can be a Clean instance or you will have inconsistencies.', 0)
  log('speed       = ' + str(speed), copyright)
  log('', copyright)
  log('foldervideo : ' + foldervideo, copyright)
  log('folderimg   : ' + folderimg, copyright)
  log('folderuw    : ' + folderuw, copyright)
  log('folderrs    : ' + folderrs, copyright)
  log('fps         : 1 image every ' + str(fpsn) + ' second', copyright)
  if noffmpeg: log('noffmpeg set', copyright)
  if sameresult: log('sameresult set', copyright)
  log('limffmpeg   = ' + str(limffmpeg) + '%', copyright)
  if delparam: log('delparam set. param.txt will be erased and ffmpeg rerun when not limffmpeg jpeg images files.')
  log('maxdiff     = ' + str(maxdiff), copyright)
  log('maxdiffuw   = ' + str(maxdiffuw), copyright)
  log('minimg      = ' + str(minimg), copyright)
  if step1: log('Step1', copyright)
  if step2: log('Step2', copyright)
  if step3: log('Step3', copyright)
  log('', 0)

  if step0:
    DisplayImages()
    sys.exit(0)

  log('************************************************************************************', 0)
  log(txtgreen + 'Initialization' + txtnocolor, 0)

  if speed < 3:
    log('Open database (can be skipped with speed=3)', 0)
    SQLopen()

  if False:
    log(txtgreen + 'Replace distance from 2 origins in case of change of those references' + txtnocolor, 0)
    CompleteDistanceOriginSql()
    log('done CompleteDistanceOriginSql', 0)
    # sys.exit()

  log('Load in memory and in SQL existing video folders list', 0)
  BoucleCount(foldervideo, folderimg, level)

  SQLexec('DROP TABLE IF EXISTS tsources;')
  SQLexec('CREATE TABLE tsources (srcshortname TEXT PRIMARY KEY, srcfld TEXT, imgfld TEXT, recordcount INTEGER);')
  argument_string = ",".join("('%s', '%s', '%s')" % (x, y, z) for (x, y, z) in srclst)
  log('INSERT INTO tsources VALUES', 1)
  SQLexec("INSERT INTO tsources VALUES" + argument_string, False, 12)
  conn.commit()
  SQLexec('CREATE INDEX IF NOT EXISTS idx_tsources_srcshortname ON public.tsources USING btree (srcshortname ASC NULLS LAST) TABLESPACE pg_default; ')

  if (speed == 1):
    SQLexec('UPDATE tsources S SET recordcount=I.num FROM (SELECT srcshortname, COUNT(*) AS num FROM timages GROUP BY srcshortname) I WHERE S.srcshortname=I.srcshortname;')
    conn.commit()
  if speed < 3:
    log('Test of source with same name (skipped with speed=3)', 0)
    SameSrcNames()
    if renamed:
      log('Some renamed video source requires to reload source files', 0)
      cpttodo = 0
      srclst = []
      BoucleCount(foldervideo, folderimg, level)
    srclst = sorted(srclst, key=sortoccurence)
    log('Align image folder to source folder by moving and deleting.', 0)
    BoucleSupp('')

  # must use Manager queue here, or will not work
  manager = multiprocessing.Manager()
  queue = manager.Queue()
  smm = SharedMemoryManager()
  smm.start()  # Start the process that manages the shared memory blocks

  log('Cleanup in SQL tables (skipped with speed=3):')
  SQLclean()

  # Step 1: Delete obsolete images
  if step1:
    log('', 0)
    log(' ' + txtgreen + 'STEP 1: ' + txtnocolor + ' ' + foldervideo, 0)
    log('************************************************************************************', 0)
    log(txtgreen + 'Parse sources to build missing images' + txtnocolor, 0)
    # BoucleFichiers(foldervideo, folderimg, 'parse', level)
    Parse()
    cur.close()
    conn.commit()
    cur = conn.cursor()

    log(txtgreen + 'Step1 done parse of ' + txtnocolor + foldervideo + ' into ' + folderimg +  ': ' + str(cptdone) + ' / ' + str(cpttodo), 0)

  if step2:
    log('', 0)
    log(' ' + txtgreen + 'STEP 2: ' + txtnocolor + ' ' + foldervideo, 0)
    log('************************************************************************************', 0)
    if speed == 1:
      log(txtgreen + 'Unwanted images analysis ' + txtnocolor + folderrs)
      UnwantedMain()
    log(txtgreen + 'Compare images together to find similar ones' + txtnocolor, 0)
    CompareSqlMain()
    log(txtgreen + 'Step2 done comparison of images in ' + txtnocolor + folderimg +  ': ' + str(cptdone) + ' / ' + str(cpttodo), 0)

  if step3:
    log('', 0)
    log(' ' + txtgreen + 'STEP 3: ' + txtnocolor + ' ' + foldervideo, 0)
    log('************************************************************************************', 0)
    log(txtgreen + 'Unwanted images analysis ' + txtnocolor + folderrs)
    UnwantedPairs()
    UnwantedMain()
    log(txtgreen + 'Analyse results with SQL requests and copy duplicate candidates in ' + txtnocolor + folderrs)
    if not (StopAsked()):
      SQLanalyse()

  log('', 0)
  if StopAsked():
    log(txtgreen + 'https://members.vixen.com/give-in ' + txtnocolor + 'key was pressed. Stopped.', 0)
  else:
    log(txtgreen + 'FINISH' + txtnocolor, 0)
  cur.close()
  conn.commit()
  smm.shutdown()
  flog.close
  tracemalloc.stop()