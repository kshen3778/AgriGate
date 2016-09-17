import hug

@hug.get()
@hug.local()
def dronePhotoBottom():
    """Returns photo from drone"""
    return {'message': 'This API is currently being worked on'}
