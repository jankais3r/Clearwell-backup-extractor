meta:
  id: exbaktor
  title: Clearwell .BAK backup
  application: Clearwell
  file-extension: bak
  endian: be
  
seq:
  - id: root_folders
    type: root_folder
    
  - id: bak_objects
    type: bak_object
    repeat: until
    repeat-until: _.end_byte == [0x45]
    
types:
  root_folder:
    seq:
      - id: object_type
        type: u1
        enum: object_type
        
      - id: root_folder_name_length
        type: u2
        
      - id: root_folder_name
        type: str
        size: root_folder_name_length
        encoding: UTF-8
        
      - id: spacer_1
        contents: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        
  bak_object:
    seq:
      - id: folder_id_start
        size: 3
        if: next_byte != [0x44] and next_byte != [0x46] and (dir_spec == [0x01, 0x44] or dir_spec == [0x01, 0x46])
        
      - id: divider
        contents: [1]
        if: next_byte2 == [0x01]
        
      - id: object_type
        type: u1
        enum: object_type
        
      - id: name_length
        type: u2
        
      - id: name
        type: str
        size: name_length
        encoding: UTF-8
        
      - id: spacer_1
        contents: [0, 0, 0, 0, 0]
        
      - id: folder_id_end
        size: 3
        
      - id: spacer_2
        if: object_type == object_type::directory
        contents: [0, 0, 0, 0, 0]
        
      - id: file_size
        if: object_type == object_type::file
        type: u8
        
      - id: spacer_3
        if: object_type == object_type::file
        contents: [0, 0]
        
      - id: timestamp
        if: object_type == object_type::file
        size: 6
        
      - id: divider_2
        if: object_type == object_type::file
        contents: [1]
        
      - id: file_contents
        if: object_type == object_type::file
        size: file_size
        
    instances:
      next_byte:
        pos: _io.pos
        size: 1
        
      next_byte2:
        pos: _io.pos
        size: 1
        
      dir_spec:
        pos: _io.pos + 3
        size: 2
        
      end_byte:
        pos: _io.pos
        size: 1
        
enums:
  object_type:
    0x44: directory
    0x46: file