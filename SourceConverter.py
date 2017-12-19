import sys, getopt, os, datetime, re, errno, boto3, HlsJobCreation

class VideoFile:
  originalFile = ''
  renameFile = ''
  s3Key = ''
  transcodePrefix = ''
  status= ''
  
  @staticmethod
  def filename(filePath):
    return os.path.splitext(os.path.split(filePath)[1])[0]

  def isVideo(self):
    filename, ext = os.path.splitext(self.originalFile)
    if ext in (".mp4",".m4v"):
      return True
    return False    

course_name = ''
course_path = ''
preview = False
video_list = []


def main(argv): 

  init(argv)

  writeSummary("\n\n\n SOURCE CONVERTER")
  writeSummary(str(datetime.datetime.today()))
  printDir(course_path)
  
  buildVideoList()
  writeSummary("\nRenaming Files")
  for videoFile in video_list:
    rename(videoFile)
  writeSummary("\n")
  if not preview:
    removeEmptyDir(course_path);

  writeSummary("\nTransfering Files")
  s3SyncCourse();
  transcode();
 
def init(argv):
  global course_name
  global course_path
  global preview

  try: 
    opts, args = getopt.getopt(argv,"c:p:",["course=","path=","preview"])
  except getopt.GetoptError:
    print ("error")
    sys.exit(2)
  for opt, arg in opts:
    if opt in ("-c", "--course"):
      course_name = arg
      findCourseSource(course_name)
    if opt in ("-p", "--path"):
      course_path = arg
      if "_source" not in course_path:
        sys.exit("Only files located in the _source directory is supported");
      getCourseName(course_path)      
    if opt in ("--preview"):
      preview = True


  print ("Starting") 
  print ('Uploading / Chunking course ', course_name)  
  print ('Course path: ', course_path)  
  writeSummary('Course Path: ' + course_path)
def printDir(path):
  print("Course files")
  for path, irs, files in os.walk(path):
    for filename in files:
      print (os.path.join(path, filename))

def findCourseSource(course_name):
  global course_path
  for path, irs, files in os.walk('./_source'):
    head, tail = os.path.split(path)
    if tail==course_name:
      course_path = path
      break

  return course_path

  print("Found course path ", course_path)

def getCourseName(course_path):
  global course_name
  head, tail = os.path.split(course_path)
  course_name = tail
  return course_name

def buildVideoList():
  global video_list
  for path, irs, files in os.walk(course_path):
    for filename in files:
      if (isVideo(filename)):
        video = VideoFile()
        video.originalFile = os.path.join(path, filename)
        video_list.append(video)

def isVideo(path):
  filename, ext = os.path.splitext(path)
  if ext in (".mp4",".m4v"):
    return True
  return False

def newFilename(path):
  print(path);
  name = ''
  
  head, tail = os.path.split(path)

  #if question, let's seed the filename with the question number
  if isQuestionVideo(path):
    name = '_' + isQuestionVideo(path).replace(".","")

  head, tail = os.path.split(head)
  name = tail + name



  return name

def rename(videoFile):
  base = newFilename(videoFile.originalFile)
  if (isVideo(videoFile.originalFile)):
    videoFile.renameFile = os.path.join(os.path.split(videoFile.originalFile)[0], base + os.path.splitext(videoFile.originalFile)[1])
    writeSummary(videoFile.originalFile + " => " + videoFile.renameFile)
    if not preview:
      os.rename(videoFile.originalFile, videoFile.renameFile)
    



def isQuestionVideo(filename):
  m = re.search(r"Q(\d)+(.)",filename,  re.I)
  if m:
    return m.group()
  else:
    return None

def removeEmptyDir(course_path):
  for path, irs, files in os.walk(course_path):
    try:
      os.rmdir(path)
      print ("removed ", path)
    except OSError as ex:
      if ex.errno == errno.ENOTEMPTY:
        print("directory not empty, couldn't remove ", path)

def writeSummary(text):
  with open(os.path.join(course_path,'summary.txt'), "a") as meta_file:
      meta_file.write(text + "\n")

def s3SyncCourse():
  s3 = boto3.resource('s3')
  writeSummary("\nS3 Bucket Info\n")
  for videoFile in video_list:
    videoFile.s3Key = cleanS3Key(os.path.join("videos",videoFile.renameFile).replace('\\','/'))
    writeSummary(videoFile.s3Key)
    if not validS3Key(videoFile.s3Key):
      writeSummary("^^^ Invalid key")
      sys.exit("key not valid -- aborting");
    if not preview:
      print("Transfering" + videoFile.renameFile)
      data = open(os.path.relpath(videoFile.renameFile), 'rb')
      s3.Bucket('mlm-o').put_object(Key=videoFile.s3Key,Body=data,ACL='private')

def cleanS3Key(s3Key):
  return s3Key.replace("./","")

def validS3Key(s3Key):
  return "videos/_source" in s3Key

def transcode():
  for videoFile in video_list:
    videoFile.transcodePrefix = os.path.split(videoFile.s3Key.replace("/_source",""))[0] + '/'
  HlsJobCreation.transcode(video_list, {'preview':preview})

if __name__ == "__main__":
  main(sys.argv[1:])


