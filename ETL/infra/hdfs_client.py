from hdfs import InsecureClient

def get_client():
    return InsecureClient('http://localhost:9870', user='big')  # hdfs에 접근하기 위한 hdfs client객체 반환