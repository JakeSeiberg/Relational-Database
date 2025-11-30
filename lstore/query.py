from lstore.table import Table, Record
from lstore.page import Page
from lstore.index import Index


class Query:
    """
    Query class with extensive debugging.
    """
    def __init__(self, table):
        self.table = table

    
    def delete(self, primary_key):
        """Delete a record by primary key."""
        try:
            rid = self.table.index.locate(self.table.key, primary_key)
            if rid is None:
                return False
            
            with self.table.pd_lock:
                if rid not in self.table.page_directory:
                    return False
                del self.table.page_directory[rid]
            
            return True
        except Exception as e:
            return False
    
    
    def insert(self, *columns):
        """Insert a new record."""
        try:
            primary_key = columns[self.table.key]
            print(f"Attempting insert with primary key: {primary_key}")

            rid = self.table.insert_row(list(columns))
            
            if rid is not None:
                print(f"Insert SUCCESS: key={primary_key}, rid={rid}")
                return True
            else:
                print(f"Insert FAILED: key={primary_key}, insert_row returned None")
                return False
        except Exception as e:
            return False

    
    def select(self, search_key, search_key_index, projected_columns_index):
        """Select a record by search key."""
        try:
            rid = self.table.index.locate(search_key_index, search_key)
            if rid is None:
                print(f"SELECT: No RID found for key {search_key}")
                return []
            
            with self.table.pd_lock:
                if rid not in self.table.page_directory:
                    print(f"SELECT: RID {rid} not in page_directory")
                    return []
                locations = list(self.table.page_directory[rid])
            
            record_values = []
            for col_idx, (page_idx, slot_idx) in enumerate(locations):
                if projected_columns_index[col_idx]:
                    value = self.table.read_column(col_idx, page_idx, slot_idx)
                    record_values.append(value)
                else:
                    record_values.append(None)
            
            return [Record(rid, search_key, record_values)]
        except Exception as e:
            return []


    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        try:
            rid = self.table.index.locate(search_key_index, search_key)
            if rid is None:
                return None
            
            with self.table.pd_lock:
                if rid not in self.table.page_directory:
                    return None
                pd_copy = list(self.table.page_directory[rid])
            
            record_values = []
            actual_version = relative_version

            if relative_version < 0:
                with self.table.vc_lock:
                    if rid not in self.table.version_chain or len(self.table.version_chain[rid]) == 0:
                        actual_version = 0
                    else:
                        version_idx = -relative_version - 1
                        if version_idx >= len(self.table.version_chain[rid]):
                            version_idx = len(self.table.version_chain[rid]) - 1
                            actual_version = -(version_idx + 1)
        
            for col_idx, is_projected in enumerate(projected_columns_index):
                if is_projected == 1:
                    if actual_version == 0:
                        page_index, record_offset = pd_copy[col_idx]
                        value = self.table.read_column(col_idx, page_index, record_offset)
                        record_values.append(value)
                    else:
                        version_idx = -actual_version - 1
                        with self.table.vc_lock:
                            if rid not in self.table.version_chain:
                                vc_copy = None
                            else:
                                vc_copy = self.table.version_chain[rid][version_idx] if version_idx < len(self.table.version_chain[rid]) else None
                        
                        if vc_copy and vc_copy[col_idx] is not None:
                            page_index, record_offset = vc_copy[col_idx]
                            page = self.table.tail_page[col_idx][page_index]
                            value = page.read(record_offset)
                            record_values.append(value)
                        else:
                            page_index, record_offset = pd_copy[col_idx]
                            value = self.table.read_column(col_idx, page_index, record_offset)
                            record_values.append(value)
                else:
                    record_values.append(None)
            
            return [Record(rid, search_key, record_values)]
        except:
            return False

    
    def update(self, primary_key, *columns):
        try:
            rid = self.table.index.locate(self.table.key, primary_key)
            if rid is None:
                return False
            
            with self.table.pd_lock:
                if rid not in self.table.page_directory:
                    return False
                old_locations = list(self.table.page_directory[rid])
            
            if columns[self.table.key] is not None:
                new_primary_key = columns[self.table.key]
                if new_primary_key != primary_key:
                    existing_rid = self.table.index.locate(self.table.key, new_primary_key)
                    if existing_rid is not None:
                        return False
            
            tail_locations = [None] * self.table.num_columns
            
            for col_idx, new_value in enumerate(columns):
                if new_value is not None:
                    page_idx, slot_idx = old_locations[col_idx]
                    old_value = self.table.read_column(col_idx, page_idx, slot_idx)
                    
                    tail_page = self.table.tail_page[col_idx][-1]
                    if not tail_page.has_capacity():
                        new_tail = Page()
                        self.table.tail_page[col_idx].append(new_tail)
                        tail_page = new_tail
                    
                    tail_page.write(old_value)
                    tail_page_idx = len(self.table.tail_page[col_idx]) - 1
                    tail_slot_idx = tail_page.num_records - 1
                    tail_locations[col_idx] = (tail_page_idx, tail_slot_idx)
                    
                    offset = slot_idx * 8
                    page = self.table.base_page[col_idx][page_idx]
                    with page.lock:
                        page.data[offset:offset + 8] = new_value.to_bytes(8, byteorder='little', signed=True)
            
            with self.table.vc_lock:
                if rid not in self.table.version_chain:
                    self.table.version_chain[rid] = []
                self.table.version_chain[rid].insert(0, tail_locations)
            
            return True
        except:
            return False

    

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        try:
            total = 0
            found = False

            rids = self.table.index.locate_range(start_range, end_range, self.table.key)

            for rid in rids:

                # ---- locate base location ----
                with self.table.pd_lock:
                    if rid not in self.table.page_directory:
                        continue
                    base_page_index, base_slot = self.table.page_directory[rid][aggregate_column_index]

                # ---- if asking for newest version ----
                if relative_version == 0:
                    value = self.table.read_column(aggregate_column_index, base_page_index, base_slot)
                    total += value
                    found = True
                    continue

                # ---- resolve relative version number ----
                version_idx = -relative_version - 1  # -1 → 0, -2 → 1, etc.

                with self.table.vc_lock:
                    chain = self.table.version_chain.get(rid, [])

                    # If version too old, clamp to oldest available
                    if version_idx >= len(chain):
                        version_idx = len(chain) - 1

                    version_entry = chain[version_idx] if chain else None

                # ---- versioned column ----
                if version_entry and version_entry[aggregate_column_index] is not None:
                    page_idx, slot_idx = version_entry[aggregate_column_index]
                    page = self.table.tail_page[aggregate_column_index][page_idx]
                    value = page.read(slot_idx)
                else:
                    # fallback: base page
                    value = self.table.read_column(aggregate_column_index, base_page_index, base_slot)

                total += value
                found = True

            return total if found else False

        except:
            return False



    def increment(self, key, column):
        try:
            r = self.select(key, self.table.key, [1] * self.table.num_columns)
            if not r or r == False:
                return False
            r = r[0]
            
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r.columns[column] + 1
            u = self.update(key, *updated_columns)
            return u
        except:
            return False
    
    
    def sum(self, start_range, end_range, aggregate_column_index):
        try:
            output = 0
            found = False
            rid_list = self.table.index.locate_range(start_range, end_range, self.table.key)
            
            for rid in rid_list:
                with self.table.pd_lock:
                    if rid in self.table.page_directory:
                        page_index, record_offset = self.table.page_directory[rid][aggregate_column_index]
                    else:
                        continue
                
                value = self.table.read_column(aggregate_column_index, page_index, record_offset)
                output += value
                found = True

            if not found:
                return False
            
            return output
        except:
            return False