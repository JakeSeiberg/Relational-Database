from lstore.table import Table
from lstore.page import Page
from lstore.lock import get_lock_manager
import os
import struct
import threading

class Database():

    def __init__(self):
        self.tables = []
        self.path = None
        self.lock_manager = get_lock_manager()  # Initialize global lock manager
        self.db_lock = threading.RLock()  # Protect database-level operations

    def open(self, path):
        """Load database from disk"""
        with self.db_lock:
            self.path = path
            
            if not os.path.exists(path):
                os.makedirs(path)
                return
            
            metadata_path = os.path.join(path, 'metadata.db')
            
            if not os.path.exists(metadata_path):
                return
            
            with open(metadata_path, 'rb') as f:
                num_tables = struct.unpack('i', f.read(4))[0]
                
                for i in range(num_tables):
                    name_len = struct.unpack('i', f.read(4))[0]
                    name = f.read(name_len).decode('utf-8')
                    num_columns = struct.unpack('i', f.read(4))[0]
                    key_index = struct.unpack('i', f.read(4))[0]
                    
                    table = Table(name, num_columns, key_index)
                    self.tables.append(table)
                    
                    self.load_table_data(path, table)

    def load_table_data(self, path, table):
        """Load all data for a specific table"""        
        table_path = os.path.join(path, table.name)
        
        if not os.path.exists(table_path):
            return
        
        for idx in range(table.num_columns):
            base_dir = os.path.join(table_path, f'base_col_{idx}')
            table.base_page[idx] = self.load_pages(base_dir)
        
        for idx in range(table.num_columns):
            tail_dir = os.path.join(table_path, f'tail_col_{idx}')
            table.tail_page[idx] = self.load_pages(tail_dir)
        
        self.load_page_directory(table_path, table)
        self.load_version_chains(table_path, table)
        
        # Rebuild index after loading data
        table.index.drop_index(table.key)
        table.index.create_index(table.key)

    def get_page_number(self, filename):
        """Extract page number from filename"""
        underscore = filename.split('_')[1] 
        dot = underscore.split('.')[0]
        return int(dot)

    def load_pages(self, directory):
        """Load all pages from a directory"""
        if not os.path.exists(directory):
            return [Page()]
        
        all_files = os.listdir(directory)
        dat_files = [f for f in all_files if f.endswith('.dat')]
        page_files = sorted(dat_files, key=self.get_page_number)
        
        pages = []
        for page_file in page_files:
            page = Page()
            with open(os.path.join(directory, page_file), 'rb') as f:
                page.num_records = struct.unpack('i', f.read(4))[0]
                page.data = bytearray(f.read(4096))
            pages.append(page)

        return pages if pages else [Page()]

    def load_page_directory(self, table_path, table):
        """Load the page directory mapping RIDs to physical locations"""
        pd_path = os.path.join(table_path, 'page_directory.dat')
        
        if not os.path.exists(pd_path):
            return
        
        with open(pd_path, 'rb') as f:
            num_entries = struct.unpack('i', f.read(4))[0]
            
            for i in range(num_entries):
                rid = struct.unpack('q', f.read(8))[0]
                positions = []
                
                for col_idx in range(table.num_columns):
                    page_idx = struct.unpack('i', f.read(4))[0]
                    slot_idx = struct.unpack('i', f.read(4))[0]
                    positions.append((page_idx, slot_idx))
                
                table.page_directory[rid] = positions
            
            if table.page_directory:
                table.rid_counter = max(table.page_directory.keys())

    def load_version_chains(self, table_path, table):
        """Load version chains for all records"""
        vc_path = os.path.join(table_path, 'version_chains.dat')
        
        if not os.path.exists(vc_path):
            return
        
        with open(vc_path, 'rb') as f:
            num_rids = struct.unpack('i', f.read(4))[0]
            
            for i in range(num_rids):
                rid = struct.unpack('q', f.read(8))[0]
                num_versions = struct.unpack('i', f.read(4))[0]
                versions = []
                
                for j in range(num_versions):
                    tail_locations = []
                    for col in range(table.num_columns):
                        has_location = struct.unpack('?', f.read(1))[0]
                        if has_location:
                            page_idx = struct.unpack('i', f.read(4))[0]
                            slot_idx = struct.unpack('i', f.read(4))[0]
                            tail_locations.append((page_idx, slot_idx))
                        else:
                            tail_locations.append(None)
                    versions.append(tail_locations)
                
                table.version_chain[rid] = versions

    def close(self):
        """Write all data to disk"""
        with self.db_lock:
            if self.path is None:
                return
            
            path = self.path
            
            if not os.path.exists(path):
                os.makedirs(path)
            
            metadata_path = os.path.join(path, 'metadata.db')
            with open(metadata_path, 'wb') as f:
                f.write(struct.pack('i', len(self.tables)))
                
                for table in self.tables:
                    name_bytes = table.name.encode('utf-8')
                    f.write(struct.pack('i', len(name_bytes)))
                    f.write(name_bytes)
                    f.write(struct.pack('i', table.num_columns))
                    f.write(struct.pack('i', table.key))
            
            for table in self.tables:
                self.save_table_data(path, table)

    def save_table_data(self, path, table):
        """Save all data for a specific table"""
        table_path = os.path.join(path, table.name)
        
        if not os.path.exists(table_path):
            os.makedirs(table_path)
        
        for idx in range(table.num_columns):
            base_dir = os.path.join(table_path, f'base_col_{idx}')
            self.save_pages(base_dir, table.base_page[idx])
        
        for idx in range(table.num_columns):
            tail_dir = os.path.join(table_path, f'tail_col_{idx}')
            self.save_pages(tail_dir, table.tail_page[idx])
        
        self.save_page_directory(table_path, table)
        self.save_version_chains(table_path, table)

    def save_pages(self, directory, pages):
        """Save all pages to a directory"""
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        for idx, page in enumerate(pages):
            page_path = os.path.join(directory, f'page_{idx}.dat')
            with open(page_path, 'wb') as f:
                f.write(struct.pack('i', page.num_records))
                f.write(page.data)

    def save_page_directory(self, table_path, table):
        """Save the page directory"""
        pd_path = os.path.join(table_path, 'page_directory.dat')
        
        with open(pd_path, 'wb') as f:
            f.write(struct.pack('i', len(table.page_directory)))
            
            for rid, positions in table.page_directory.items():
                f.write(struct.pack('q', rid))
                
                for page_idx, slot_idx in positions:
                    f.write(struct.pack('i', page_idx))
                    f.write(struct.pack('i', slot_idx))

    def save_version_chains(self, table_path, table):
        """Save version chains"""
        vc_path = os.path.join(table_path, 'version_chains.dat')
        
        with open(vc_path, 'wb') as f:
            f.write(struct.pack('i', len(table.version_chain)))
            
            for rid, versions in table.version_chain.items():
                f.write(struct.pack('q', rid))
                f.write(struct.pack('i', len(versions)))
                
                for tail_locs in versions:
                    for col_idx in range(table.num_columns):
                        if tail_locs[col_idx] is not None:
                            f.write(struct.pack('?', True))
                            page_idx, slot_idx = tail_locs[col_idx]
                            f.write(struct.pack('i', page_idx))
                            f.write(struct.pack('i', slot_idx))
                        else:
                            f.write(struct.pack('?', False))

    def create_table(self, name, num_columns, key_index):
        """
        Creates a new table
        :param name: string
        :param num_columns: int
        :param key: int
        """
        with self.db_lock:
            table = Table(name, num_columns, key_index)
            self.tables.append(table)
            return table
    
    def drop_table(self, name):
        """Deletes the specified table"""
        with self.db_lock:
            for table in self.tables:
                if table.name == name:
                    self.tables.remove(table)
                    print("table dropped")
                    return 1
            print("table not found")
            return -1

    def get_table(self, name):
        """Returns table with the passed name"""
        with self.db_lock:
            for table in self.tables:
                if table.name == name:
                    print("table was found")
                    return table
            print('table not found')
            return None