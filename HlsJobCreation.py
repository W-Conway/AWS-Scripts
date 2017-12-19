import hashlib, json, boto3, os

#preset keys from aws.  
hls_64k_audio_preset_id = '1351620000001-200071';
hls_0400k_preset_id     = '1351620000001-200050';
hls_0600k_preset_id     = '1351620000001-200040';
hls_1000k_preset_id     = '1351620000001-200030';
hls_1500k_preset_id     = '1351620000001-200020';
hls_2000k_preset_id     = '1351620000001-200010';

transcoder_client = ''

def transcode(videoFiles, options):
  global transcoder_client

  if ('region' not in options):
    options['region'] = 'us-east-1'  

  if ('pipeline' not in options):
    options['pipeline'] = '1458137541185-5jtc9s'  #default pipeline

  if ('segment_duration' not in options):
    options['segment_duration'] = '10'

  transcoder_client = boto3.client('elastictranscoder', options['region'])

  for videoFile in videoFiles:
    if videoFile.isVideo():
      transcodeFile(videoFile, options)

def getOutPath(videoFile, preset):
  filename = videoFile.filename(videoFile.renameFile)  
  return videoFile.transcodePrefix + preset + "/" + filename +"/"+filename+"_"

def playlistPath(videoFile):
  return videoFile.transcodePrefix + videoFile.filename(videoFile.renameFile) 

def transcodeFile(videoFile, options):


    # HLS Presets that will be used to create an adaptive bitrate playlist.


    # HLS Segment duration that will be targeted.
    segment_duration = options['segment_duration']

    #All outputs will have this prefix prepended to their output key.
    output_key_prefix = videoFile.transcodePrefix
        
    # Setup the job input using the provided input key.
    job_input = videoFile.s3Key

    # Setup the job outputs using the HLS presets.
    output_key = hashlib.sha256(videoFile.s3Key.encode('utf-8')).hexdigest()
    hls_audio = {
        'Key' : getOutPath(videoFile, 'hlsAudio'),
        'PresetId' : hls_64k_audio_preset_id,
        'SegmentDuration' : segment_duration
    }
    hls_400k = {
        'Key' : getOutPath(videoFile, 'hls0400k'),
        'PresetId' : hls_0400k_preset_id,
        'SegmentDuration' : segment_duration
    }
    hls_600k = {
        'Key' : getOutPath(videoFile, 'hls0600k'),
        'PresetId' : hls_0600k_preset_id,
        'SegmentDuration' : segment_duration
    }
    hls_1000k = {
        'Key' : getOutPath(videoFile, 'hls1000k'),
        'PresetId' : hls_1000k_preset_id,
        'SegmentDuration' : segment_duration
    }
    hls_1500k = {
        'Key' : getOutPath(videoFile, 'hls1500k'),
        'PresetId' : hls_1500k_preset_id,
        'SegmentDuration' : segment_duration
    }
    hls_2000k = {
        'Key' : getOutPath(videoFile, 'hls2000k'),
        'PresetId' : hls_2000k_preset_id,
        'SegmentDuration' : segment_duration
    }
    job_outputs = [ hls_400k, hls_600k, hls_1000k]

    # Setup master playlist which can be used to play using adaptive bitrate.
    playlist = [{
        'Name' : playlistPath(videoFile),
        'Format' : 'HLSv3',
        'OutputKeys' : list(map(lambda x: x['Key'], job_outputs))
    }]
    # Creating the job.

    print(videoFile.transcodePrefix)

    preview = options['preview']
    if not preview:
      create_job_result=transcoder_client.create_job(
        PipelineId= options['pipeline'],
        Input={
          'Key':job_input,
          'FrameRate': 'auto',
          'Resolution': 'auto',
          'AspectRatio': 'auto',
          'Interlaced': 'auto',
          'Container': 'auto'
        },
        Outputs= job_outputs,
        Playlists= playlist )
      print ('HLS job has been created: ', json.dumps(create_job_result['Job'], indent=4, sort_keys=True))
