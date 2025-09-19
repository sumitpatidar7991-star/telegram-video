import sqlite3
import random
import uuid
import os
from datetime import datetime
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

class Database:
    def __init__(self):
        # Check if DATABASE_URL is set (Render PostgreSQL)
        self.database_url = os.getenv('DATABASE_URL')
        
        if self.database_url and POSTGRES_AVAILABLE:
            print("📊 Using PostgreSQL database")
            self.db_type = 'postgresql'
            self.conn = psycopg2.connect(self.database_url)
            self.conn.autocommit = True
        else:
            print("📊 Using SQLite database (local)")
            self.db_type = 'sqlite'
            self.conn = sqlite3.connect('videos.db', check_same_thread=False)
        
        self.create_tables()

    def get_cursor(self):
        """Get appropriate cursor based on database type"""
        if self.db_type == 'postgresql':
            return self.conn.cursor(cursor_factory=RealDictCursor)
        else:
            return self.conn.cursor()

    def commit(self):
        """Commit transaction if needed"""
        if self.db_type == 'sqlite':
            self.conn.commit()
        # PostgreSQL uses autocommit

    def create_tables(self):
        """Create all necessary tables if they don't exist"""
        cursor = self.get_cursor()
        
        if self.db_type == 'postgresql':
            # PostgreSQL table creation
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS videos (
                    id TEXT PRIMARY KEY,
                    file_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS banned_users (
                    user_id BIGINT PRIMARY KEY,
                    banned_by BIGINT,
                    banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    reason TEXT,
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_activity (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    action TEXT,
                    details TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS video_analytics (
                    id SERIAL PRIMARY KEY,
                    video_id TEXT,
                    user_id BIGINT,
                    action TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (video_id) REFERENCES videos (id),
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS scheduled_broadcasts (
                    id SERIAL PRIMARY KEY,
                    admin_id BIGINT,
                    target_channel TEXT,
                    content_type TEXT,
                    content TEXT,
                    media_file_id TEXT,
                    scheduled_time TIMESTAMP,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (admin_id) REFERENCES users (user_id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS message_templates (
                    id SERIAL PRIMARY KEY,
                    name TEXT UNIQUE,
                    content TEXT,
                    created_by BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (created_by) REFERENCES users (user_id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS categories (
                    id SERIAL PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    description TEXT,
                    color TEXT DEFAULT '#007ACC',
                    created_by BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (created_by) REFERENCES users (user_id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS video_categories (
                    id SERIAL PRIMARY KEY,
                    video_id TEXT NOT NULL,
                    category_id INTEGER NOT NULL,
                    assigned_by BIGINT,
                    assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (video_id) REFERENCES videos (id) ON DELETE CASCADE,
                    FOREIGN KEY (category_id) REFERENCES categories (id) ON DELETE CASCADE,
                    FOREIGN KEY (assigned_by) REFERENCES users (user_id),
                    UNIQUE(video_id, category_id)
                )
            ''')
        else:
            # SQLite table creation (original)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS videos (
                    id TEXT PRIMARY KEY,
                    file_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS banned_users (
                    user_id INTEGER PRIMARY KEY,
                    banned_by INTEGER,
                    banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    reason TEXT,
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_activity (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    action TEXT,
                    details TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS video_analytics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id TEXT,
                    user_id INTEGER,
                    action TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (video_id) REFERENCES videos (id),
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS scheduled_broadcasts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    admin_id INTEGER,
                    target_channel TEXT,
                    content_type TEXT,
                    content TEXT,
                    media_file_id TEXT,
                    scheduled_time TIMESTAMP,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (admin_id) REFERENCES users (user_id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS message_templates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE,
                    content TEXT,
                    created_by INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (created_by) REFERENCES users (user_id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS categories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    description TEXT,
                    color TEXT DEFAULT '#007ACC',
                    created_by INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (created_by) REFERENCES users (user_id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS video_categories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id TEXT NOT NULL,
                    category_id INTEGER NOT NULL,
                    assigned_by INTEGER,
                    assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (video_id) REFERENCES videos (id) ON DELETE CASCADE,
                    FOREIGN KEY (category_id) REFERENCES categories (id) ON DELETE CASCADE,
                    FOREIGN KEY (assigned_by) REFERENCES users (user_id),
                    UNIQUE(video_id, category_id)
                )
            ''')
        
        # Migrate existing tables if needed
        self.migrate_users_table()
        self.migrate_categories_system()
        self.commit()

    def migrate_users_table(self):
        """Migrate existing users table to include new columns"""
        cursor = self.get_cursor()
        try:
            if self.db_type == 'sqlite':
                # Check if username column exists
                cursor.execute("PRAGMA table_info(users)")
                columns = [column[1] for column in cursor.fetchall()]
                
                if 'username' not in columns:
                    cursor.execute('ALTER TABLE users ADD COLUMN username TEXT')
                    print("✅ Added username column to users table")
                
                if 'first_name' not in columns:
                    cursor.execute('ALTER TABLE users ADD COLUMN first_name TEXT')
                    print("✅ Added first_name column to users table")
                
                if 'joined_at' not in columns:
                    cursor.execute('ALTER TABLE users ADD COLUMN joined_at TIMESTAMP')
                    cursor.execute("UPDATE users SET joined_at = datetime('now') WHERE joined_at IS NULL")
                    print("✅ Added joined_at column to users table")
                
                if 'last_activity' not in columns:
                    cursor.execute('ALTER TABLE users ADD COLUMN last_activity TIMESTAMP')
                    cursor.execute("UPDATE users SET last_activity = datetime('now') WHERE last_activity IS NULL")
                    print("✅ Added last_activity column to users table")
            else:
                # PostgreSQL migration logic would go here if needed
                print("✅ PostgreSQL migration completed")
                
        except Exception as e:
            print(f"⚠️ Database migration warning: {e}")

    def migrate_categories_system(self):
        """Migrate existing database to include categories system"""
        cursor = self.get_cursor()
        try:
            if self.db_type == 'sqlite':
                # Check if categories table exists
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='categories'")
                if not cursor.fetchone():
                    print("✅ Categories system tables will be created")
                    return
            else:
                # PostgreSQL check
                cursor.execute("SELECT tablename FROM pg_tables WHERE tablename='categories'")
                if not cursor.fetchone():
                    print("✅ Categories system tables will be created")
                    return
            
            print("✅ Categories system migration completed")
            
        except Exception as e:
            print(f"⚠️ Categories migration warning: {e}")

    def add_user(self, user_id, username=None, first_name=None):
        """Add a user to the database if they don't exist"""
        cursor = self.get_cursor()
        if self.db_type == 'postgresql':
            cursor.execute('''
                INSERT INTO users (user_id, username, first_name) 
                VALUES (%s, %s, %s) ON CONFLICT (user_id) DO NOTHING
            ''', (user_id, username, first_name))
            # Update last activity for existing users
            cursor.execute('''
                UPDATE users SET last_activity = CURRENT_TIMESTAMP, 
                username = COALESCE(%s, username), 
                first_name = COALESCE(%s, first_name) 
                WHERE user_id = %s
            ''', (username, first_name, user_id))
        else:
            cursor.execute('''
                INSERT OR IGNORE INTO users (user_id, username, first_name) 
                VALUES (?, ?, ?)
            ''', (user_id, username, first_name))
            # Update last activity for existing users
            cursor.execute('''
                UPDATE users SET last_activity = CURRENT_TIMESTAMP, 
                username = COALESCE(?, username), 
                first_name = COALESCE(?, first_name) 
                WHERE user_id = ?
            ''', (username, first_name, user_id))
        self.commit()

    def remove_user(self, user_id):
        """Remove a user from the database"""
        cursor = self.get_cursor()
        placeholder = '%s' if self.db_type == 'postgresql' else '?'
        cursor.execute(f'DELETE FROM users WHERE user_id = {placeholder}', (user_id,))
        self.commit()

    def get_all_users(self):
        """Retrieve all users with details"""
        cursor = self.get_cursor()
        cursor.execute('''
            SELECT user_id, username, first_name, joined_at, last_activity 
            FROM users ORDER BY joined_at DESC
        ''')
        results = cursor.fetchall()
        if self.db_type == 'postgresql':
            return [dict(r) for r in results]
        else:
            return [{
                'user_id': r[0], 
                'username': r[1], 
                'first_name': r[2], 
                'joined_at': r[3], 
                'last_activity': r[4]
            } for r in results]

    def get_active_users(self):
        """Retrieve all non-banned users for broadcasts"""
        cursor = self.get_cursor()
        cursor.execute('''
            SELECT u.user_id, u.username, u.first_name, u.joined_at, u.last_activity 
            FROM users u
            LEFT JOIN banned_users b ON u.user_id = b.user_id
            WHERE b.user_id IS NULL
            ORDER BY u.joined_at DESC
        ''')
        results = cursor.fetchall()
        if self.db_type == 'postgresql':
            return [dict(r) for r in results]
        else:
            return [{
                'user_id': r[0], 
                'username': r[1], 
                'first_name': r[2], 
                'joined_at': r[3], 
                'last_activity': r[4]
            } for r in results]

    def add_video(self, file_id, name, description):
        """Add a video to the database with a random UUID"""
        video_id = str(uuid.uuid4()) # Generate a unique ID
        cursor = self.get_cursor()
        placeholder = '%s' if self.db_type == 'postgresql' else '?'
        cursor.execute(
            f'INSERT INTO videos (id, file_id, name, description) VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder})',
            (video_id, file_id, name, description)
        )
        self.commit()
        return video_id

    def get_video_by_id(self, video_id):
        """Retrieve a video by its ID"""
        cursor = self.get_cursor()
        placeholder = '%s' if self.db_type == 'postgresql' else '?'
        cursor.execute(f'SELECT id, file_id, name, description FROM videos WHERE id = {placeholder}', (video_id,))
        result = cursor.fetchone()
        if result:
            if self.db_type == 'postgresql':
                return dict(result)
            else:
                return {'id': result[0], 'file_id': result[1], 'name': result[2], 'description': result[3]}
        return None

    def get_all_videos(self):
        """Retrieve all videos"""
        cursor = self.get_cursor()
        cursor.execute('SELECT id, file_id, name, description FROM videos')
        results = cursor.fetchall()
        if self.db_type == 'postgresql':
            return [dict(r) for r in results]
        else:
            return [{'id': r[0], 'file_id': r[1], 'name': r[2], 'description': r[3]} for r in results]

    def get_random_video(self):
        """Retrieve a random video"""
        videos = self.get_all_videos()
        return random.choice(videos) if videos else None

    def search_videos(self, query):
        """Search videos by name or description"""
        cursor = self.get_cursor()
        query_param = f'%{query}%'
        if self.db_type == 'postgresql':
            cursor.execute(
                'SELECT id, file_id, name, description FROM videos WHERE name ILIKE %s OR description ILIKE %s',
                (query_param, query_param)
            )
            return [dict(r) for r in cursor.fetchall()]
        else:
            cursor.execute(
                'SELECT id, file_id, name, description FROM videos WHERE name LIKE ? OR description LIKE ?',
                (query_param, query_param)
            )
            results = cursor.fetchall()
            return [{'id': r[0], 'file_id': r[1], 'name': r[2], 'description': r[3]} for r in results]

    def delete_video(self, video_id):
        """Delete a video by ID"""
        cursor = self.get_cursor()
        placeholder = '%s' if self.db_type == 'postgresql' else '?'
        cursor.execute(f'DELETE FROM videos WHERE id = {placeholder}', (video_id,))
        self.commit()
        return cursor.rowcount > 0

    # Admin Statistics Methods
    def get_user_count(self):
        """Get total number of users"""
        cursor = self.get_cursor()
        cursor.execute('SELECT COUNT(*) FROM users')
        return cursor.fetchone()[0]

    def get_video_count(self):
        """Get total number of videos"""
        cursor = self.get_cursor()
        cursor.execute('SELECT COUNT(*) FROM videos')
        return cursor.fetchone()[0]

    def get_video_stats(self):
        """Get detailed video statistics"""
        cursor = self.get_cursor()
        if self.db_type == 'postgresql':
            cursor.execute('SELECT COUNT(*) as total, COUNT(CASE WHEN description != \'\' THEN 1 END) as with_desc FROM videos')
            result = cursor.fetchone()
            return {'total': result['total'], 'with_description': result['with_desc']}
        else:
            cursor.execute('SELECT COUNT(*) as total, COUNT(CASE WHEN description != "" THEN 1 END) as with_desc FROM videos')
            result = cursor.fetchone()
            return {'total': result[0], 'with_description': result[1]}

    def get_recent_videos(self, limit=5):
        """Get most recently added videos"""
        cursor = self.get_cursor()
        if self.db_type == 'postgresql':
            cursor.execute('SELECT id, name, description FROM videos ORDER BY created_at DESC LIMIT %s', (limit,))
            return [dict(r) for r in cursor.fetchall()]
        else:
            cursor.execute('SELECT id, name, description FROM videos ORDER BY rowid DESC LIMIT ?', (limit,))
            results = cursor.fetchall()
            return [{'id': r[0], 'name': r[1], 'description': r[2]} for r in results]

    def clear_all_users(self):
        """Clear all users (admin only)"""
        cursor = self.get_cursor()
        cursor.execute('DELETE FROM users')
        self.commit()
        return cursor.rowcount

    def clear_all_videos(self):
        """Clear all videos (admin only)"""
        cursor = self.get_cursor()
        cursor.execute('DELETE FROM videos')
        self.commit()
        return cursor.rowcount

    # ===== ENHANCED USER MANAGEMENT METHODS =====

    def ban_user(self, user_id, banned_by, reason="No reason provided"):
        """Ban a user"""
        cursor = self.get_cursor()
        if self.db_type == 'postgresql':
            cursor.execute('''
                INSERT INTO banned_users (user_id, banned_by, reason)
                VALUES (%s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET
                banned_by = EXCLUDED.banned_by, reason = EXCLUDED.reason, banned_at = CURRENT_TIMESTAMP
            ''', (user_id, banned_by, reason))
        else:
            cursor.execute('''
                INSERT OR REPLACE INTO banned_users (user_id, banned_by, reason)
                VALUES (?, ?, ?)
            ''', (user_id, banned_by, reason))
        self.log_user_activity(user_id, "banned", f"Banned by {banned_by}: {reason}")
        self.commit()
        return True

    def unban_user(self, user_id):
        """Unban a user"""
        cursor = self.get_cursor()
        placeholder = '%s' if self.db_type == 'postgresql' else '?'
        cursor.execute(f'DELETE FROM banned_users WHERE user_id = {placeholder}', (user_id,))
        self.log_user_activity(user_id, "unbanned", "User unbanned")
        self.commit()
        return cursor.rowcount > 0

    def is_user_banned(self, user_id):
        """Check if a user is banned"""
        cursor = self.get_cursor()
        placeholder = '%s' if self.db_type == 'postgresql' else '?'
        cursor.execute(f'SELECT user_id FROM banned_users WHERE user_id = {placeholder}', (user_id,))
        return cursor.fetchone() is not None

    def get_banned_users(self):
        """Get all banned users with details"""
        cursor = self.get_cursor()
        cursor.execute('''
            SELECT b.user_id, u.username, u.first_name, b.banned_by, 
                   b.banned_at, b.reason
            FROM banned_users b
            LEFT JOIN users u ON b.user_id = u.user_id
            ORDER BY b.banned_at DESC
        ''')
        results = cursor.fetchall()
        if self.db_type == 'postgresql':
            return [dict(r) for r in results]
        else:
            return [{
                'user_id': r[0], 
                'username': r[1], 
                'first_name': r[2], 
                'banned_by': r[3],
                'banned_at': r[4], 
                'reason': r[5]
            } for r in results]

    def log_user_activity(self, user_id, action, details=""):
        """Log user activity"""
        cursor = self.get_cursor()
        if self.db_type == 'postgresql':
            cursor.execute('''
                INSERT INTO user_activity (user_id, action, details)
                VALUES (%s, %s, %s)
            ''', (user_id, action, details))
        else:
            cursor.execute('''
                INSERT INTO user_activity (user_id, action, details)
                VALUES (?, ?, ?)
            ''', (user_id, action, details))
        self.commit()

    def get_user_activity(self, user_id, limit=10):
        """Get activity history for a specific user"""
        cursor = self.get_cursor()
        if self.db_type == 'postgresql':
            cursor.execute('''
                SELECT action, details, timestamp 
                FROM user_activity 
                WHERE user_id = %s 
                ORDER BY timestamp DESC 
                LIMIT %s
            ''', (user_id, limit))
            return [dict(r) for r in cursor.fetchall()]
        else:
            cursor.execute('''
                SELECT action, details, timestamp 
                FROM user_activity 
                WHERE user_id = ? 
                ORDER BY timestamp DESC 
                LIMIT ?
            ''', (user_id, limit))
            results = cursor.fetchall()
            return [{
                'action': r[0], 
                'details': r[1], 
                'timestamp': r[2]
            } for r in results]

    def get_recent_activity(self, limit=20):
        """Get recent activity across all users"""
        cursor = self.get_cursor()
        if self.db_type == 'postgresql':
            cursor.execute('''
                SELECT a.user_id, u.username, u.first_name, a.action, 
                       a.details, a.timestamp 
                FROM user_activity a
                LEFT JOIN users u ON a.user_id = u.user_id
                ORDER BY a.timestamp DESC 
                LIMIT %s
            ''', (limit,))
            return [dict(r) for r in cursor.fetchall()]
        else:
            cursor.execute('''
                SELECT a.user_id, u.username, u.first_name, a.action, 
                       a.details, a.timestamp 
                FROM user_activity a
                LEFT JOIN users u ON a.user_id = u.user_id
                ORDER BY a.timestamp DESC 
                LIMIT ?
            ''', (limit,))
            results = cursor.fetchall()
            return [{
                'user_id': r[0], 
                'username': r[1], 
                'first_name': r[2], 
                'action': r[3],
                'details': r[4], 
                'timestamp': r[5]
            } for r in results]

    def search_users(self, query):
        """Search users by username or first name"""
        cursor = self.get_cursor()
        query_param = f'%{query}%'
        if self.db_type == 'postgresql':
            cursor.execute('''
                SELECT user_id, username, first_name, joined_at, last_activity
                FROM users 
                WHERE username ILIKE %s OR first_name ILIKE %s OR CAST(user_id AS TEXT) ILIKE %s
                ORDER BY last_activity DESC
            ''', (query_param, query_param, query_param))
            return [dict(r) for r in cursor.fetchall()]
        else:
            cursor.execute('''
                SELECT user_id, username, first_name, joined_at, last_activity
                FROM users 
                WHERE username LIKE ? OR first_name LIKE ? OR CAST(user_id AS TEXT) LIKE ?
                ORDER BY last_activity DESC
            ''', (query_param, query_param, query_param))
            results = cursor.fetchall()
            return [{
                'user_id': r[0], 
                'username': r[1], 
                'first_name': r[2], 
                'joined_at': r[3], 
                'last_activity': r[4]
            } for r in results]

    def get_user_stats_detailed(self):
        """Get detailed user statistics"""
        cursor = self.get_cursor()
        
        # Basic counts
        cursor.execute('SELECT COUNT(*) FROM users')
        total_users = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM banned_users')
        banned_count = cursor.fetchone()[0]
        
        # Active users (last 7 days)
        if self.db_type == 'postgresql':
            cursor.execute('''
                SELECT COUNT(*) FROM users 
                WHERE last_activity > CURRENT_TIMESTAMP - INTERVAL '7 days'
            ''')
            active_users = cursor.fetchone()[0]
            
            cursor.execute('''
                SELECT COUNT(*) FROM users 
                WHERE joined_at > CURRENT_TIMESTAMP - INTERVAL '7 days'
            ''')
            new_users = cursor.fetchone()[0]
        else:
            cursor.execute('''
                SELECT COUNT(*) FROM users 
                WHERE last_activity > datetime('now', '-7 days')
            ''')
            active_users = cursor.fetchone()[0]
            
            cursor.execute('''
                SELECT COUNT(*) FROM users 
                WHERE joined_at > datetime('now', '-7 days')
            ''')
            new_users = cursor.fetchone()[0]
        
        return {
            'total_users': total_users,
            'banned_users': banned_count,
            'active_users_7d': active_users,
            'new_users_7d': new_users,
            'active_users': total_users - banned_count
        }

    def bulk_ban_users(self, user_ids, banned_by, reason="Bulk ban"):
        """Ban multiple users at once"""
        cursor = self.get_cursor()
        banned_count = 0
        for user_id in user_ids:
            try:
                if self.db_type == 'postgresql':
                    cursor.execute('''
                        INSERT INTO banned_users (user_id, banned_by, reason)
                        VALUES (%s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET
                        banned_by = EXCLUDED.banned_by, reason = EXCLUDED.reason, banned_at = CURRENT_TIMESTAMP
                    ''', (user_id, banned_by, reason))
                else:
                    cursor.execute('''
                        INSERT OR REPLACE INTO banned_users (user_id, banned_by, reason)
                        VALUES (?, ?, ?)
                    ''', (user_id, banned_by, reason))
                self.log_user_activity(user_id, "bulk_banned", f"Bulk banned by {banned_by}: {reason}")
                banned_count += 1
            except Exception as e:
                print(f"Failed to ban user {user_id}: {e}")
        self.commit()
        return banned_count

    # ===== VIDEO ANALYTICS METHODS =====
    
    def log_video_view(self, video_id, user_id):
        """Log a video view"""
        cursor = self.get_cursor()
        if self.db_type == 'postgresql':
            cursor.execute('''
                INSERT INTO video_analytics (video_id, user_id, action)
                VALUES (%s, %s, 'view')
            ''', (video_id, user_id))
        else:
            cursor.execute('''
                INSERT INTO video_analytics (video_id, user_id, action)
                VALUES (?, ?, 'view')
            ''', (video_id, user_id))
        self.commit()

    def get_video_analytics(self, video_id):
        """Get analytics for a specific video"""
        cursor = self.get_cursor()
        placeholder = '%s' if self.db_type == 'postgresql' else '?'
        cursor.execute(f'''
            SELECT COUNT(*) as total_views,
                   COUNT(DISTINCT user_id) as unique_viewers,
                   MIN(timestamp) as first_view,
                   MAX(timestamp) as last_view
            FROM video_analytics 
            WHERE video_id = {placeholder} AND action = 'view'
        ''', (video_id,))
        result = cursor.fetchone()
        if self.db_type == 'postgresql':
            return dict(result)
        else:
            return {
                'total_views': result[0],
                'unique_viewers': result[1], 
                'first_view': result[2],
                'last_view': result[3]
            }

    def get_popular_videos(self, limit=10):
        """Get most popular videos by view count"""
        cursor = self.get_cursor()
        limit_placeholder = '%s' if self.db_type == 'postgresql' else '?'
        cursor.execute(f'''
            SELECT v.id, v.name, v.description, COUNT(va.id) as view_count
            FROM videos v
            LEFT JOIN video_analytics va ON v.id = va.video_id AND va.action = 'view'
            GROUP BY v.id, v.name, v.description
            ORDER BY view_count DESC
            LIMIT {limit_placeholder}
        ''', (limit,))
        results = cursor.fetchall()
        if self.db_type == 'postgresql':
            return [dict(r) for r in results]
        else:
            return [{
                'id': r[0], 
                'name': r[1], 
                'description': r[2], 
                'view_count': r[3]
            } for r in results]

    def get_analytics_summary(self):
        """Get overall analytics summary"""
        cursor = self.get_cursor()
        
        # Total views
        cursor.execute('SELECT COUNT(*) FROM video_analytics WHERE action = \'view\'')
        total_views = cursor.fetchone()[0]
        
        # Views today
        if self.db_type == 'postgresql':
            cursor.execute('''
                SELECT COUNT(*) FROM video_analytics 
                WHERE action = 'view' AND DATE(timestamp) = CURRENT_DATE
            ''')
        else:
            cursor.execute('''
                SELECT COUNT(*) FROM video_analytics 
                WHERE action = "view" AND DATE(timestamp) = DATE('now')
            ''')
        views_today = cursor.fetchone()[0]
        
        # Most viewed video
        cursor.execute('''
            SELECT v.name, COUNT(va.id) as views 
            FROM videos v
            LEFT JOIN video_analytics va ON v.id = va.video_id AND va.action = 'view'
            GROUP BY v.id, v.name
            ORDER BY views DESC
            LIMIT 1
        ''')
        top_video = cursor.fetchone()
        
        if self.db_type == 'postgresql':
            return {
                'total_views': total_views,
                'views_today': views_today,
                'top_video': {'name': top_video['name'], 'views': top_video['views']} if top_video else None
            }
        else:
            return {
                'total_views': total_views,
                'views_today': views_today,
                'top_video': {'name': top_video[0], 'views': top_video[1]} if top_video else None
            }

    # ===== MESSAGE TEMPLATES METHODS =====
    
    def add_template(self, name, content, created_by):
        """Add a message template"""
        cursor = self.get_cursor()
        if self.db_type == 'postgresql':
            cursor.execute('''
                INSERT INTO message_templates (name, content, created_by)
                VALUES (%s, %s, %s) ON CONFLICT (name) DO UPDATE SET
                content = EXCLUDED.content, created_by = EXCLUDED.created_by, created_at = CURRENT_TIMESTAMP
            ''', (name, content, created_by))
        else:
            cursor.execute('''
                INSERT OR REPLACE INTO message_templates (name, content, created_by)
                VALUES (?, ?, ?)
            ''', (name, content, created_by))
        self.commit()
        return cursor.rowcount > 0

    def get_templates(self):
        """Get all message templates"""
        cursor = self.get_cursor()
        cursor.execute('''
            SELECT id, name, content, created_by, created_at
            FROM message_templates
            ORDER BY name
        ''')
        results = cursor.fetchall()
        if self.db_type == 'postgresql':
            return [dict(r) for r in results]
        else:
            return [{
                'id': r[0],
                'name': r[1], 
                'content': r[2], 
                'created_by': r[3],
                'created_at': r[4]
            } for r in results]

    def get_template_by_name(self, name):
        """Get a template by name"""
        cursor = self.get_cursor()
        placeholder = '%s' if self.db_type == 'postgresql' else '?'
        cursor.execute(f'''
            SELECT id, name, content, created_by, created_at
            FROM message_templates
            WHERE name = {placeholder}
        ''', (name,))
        result = cursor.fetchone()
        if result:
            if self.db_type == 'postgresql':
                return dict(result)
            else:
                return {
                    'id': result[0],
                    'name': result[1], 
                    'content': result[2], 
                    'created_by': result[3],
                    'created_at': result[4]
                }
        return None

    def delete_template(self, name):
        """Delete a template by name"""
        cursor = self.get_cursor()
        placeholder = '%s' if self.db_type == 'postgresql' else '?'
        cursor.execute(f'DELETE FROM message_templates WHERE name = {placeholder}', (name,))
        self.commit()
        return cursor.rowcount > 0

    # ===== SCHEDULED BROADCASTS METHODS =====
    
    def add_scheduled_broadcast(self, admin_id, target_channel, content_type, content, 
                               media_file_id, scheduled_time):
        """Add a scheduled broadcast"""
        cursor = self.get_cursor()
        if self.db_type == 'postgresql':
            cursor.execute('''
                INSERT INTO scheduled_broadcasts 
                (admin_id, target_channel, content_type, content, media_file_id, scheduled_time)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (admin_id, target_channel, content_type, content, media_file_id, scheduled_time))
        else:
            cursor.execute('''
                INSERT INTO scheduled_broadcasts 
                (admin_id, target_channel, content_type, content, media_file_id, scheduled_time)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (admin_id, target_channel, content_type, content, media_file_id, scheduled_time))
        self.commit()
        return cursor.lastrowid

    def get_pending_broadcasts(self):
        """Get all pending scheduled broadcasts"""
        cursor = self.get_cursor()
        if self.db_type == 'postgresql':
            cursor.execute('''
                SELECT id, admin_id, target_channel, content_type, content, media_file_id, scheduled_time
                FROM scheduled_broadcasts 
                WHERE status = 'pending' AND scheduled_time <= CURRENT_TIMESTAMP
                ORDER BY scheduled_time ASC
            ''')
            return [dict(r) for r in cursor.fetchall()]
        else:
            cursor.execute('''
                SELECT id, admin_id, target_channel, content_type, content, media_file_id, scheduled_time
                FROM scheduled_broadcasts 
                WHERE status = 'pending' AND scheduled_time <= datetime('now')
                ORDER BY scheduled_time ASC
            ''')
            results = cursor.fetchall()
            return [{
                'id': r[0], 
                'admin_id': r[1], 
                'target_channel': r[2], 
                'content_type': r[3],
                'content': r[4], 
                'media_file_id': r[5],
                'scheduled_time': r[6]
            } for r in results]

    def update_broadcast_status(self, broadcast_id, status):
        """Update the status of a scheduled broadcast"""
        cursor = self.get_cursor()
        if self.db_type == 'postgresql':
            cursor.execute('UPDATE scheduled_broadcasts SET status = %s WHERE id = %s', (status, broadcast_id))
        else:
            cursor.execute('UPDATE scheduled_broadcasts SET status = ? WHERE id = ?', (status, broadcast_id))
        self.commit()
        return cursor.rowcount > 0

    def get_scheduled_broadcasts(self, admin_id=None, limit=50):
        """Get scheduled broadcasts, optionally filtered by admin"""
        cursor = self.get_cursor()
        if admin_id:
            if self.db_type == 'postgresql':
                cursor.execute('''
                    SELECT id, admin_id, target_channel, content_type, content, 
                           media_file_id, scheduled_time, status, created_at
                    FROM scheduled_broadcasts 
                    WHERE admin_id = %s
                    ORDER BY scheduled_time DESC
                    LIMIT %s
                ''', (admin_id, limit))
            else:
                cursor.execute('''
                    SELECT id, admin_id, target_channel, content_type, content, 
                           media_file_id, scheduled_time, status, created_at
                    FROM scheduled_broadcasts 
                    WHERE admin_id = ?
                    ORDER BY scheduled_time DESC
                    LIMIT ?
                ''', (admin_id, limit))
        else:
            limit_placeholder = '%s' if self.db_type == 'postgresql' else '?'
            cursor.execute(f'''
                SELECT id, admin_id, target_channel, content_type, content, 
                       media_file_id, scheduled_time, status, created_at
                FROM scheduled_broadcasts 
                ORDER BY scheduled_time DESC
                LIMIT {limit_placeholder}
            ''', (limit,))
        
        results = cursor.fetchall()
        if self.db_type == 'postgresql':
            return [dict(r) for r in results]
        else:
            return [{
                'id': r[0], 
                'admin_id': r[1], 
                'target_channel': r[2], 
                'content_type': r[3],
                'content': r[4], 
                'media_file_id': r[5],
                'scheduled_time': r[6], 
                'status': r[7], 
                'created_at': r[8]
            } for r in results]

    # ===== CATEGORIES METHODS =====
    
    def add_category(self, name, description, color, created_by):
        """Add a new category"""
        cursor = self.get_cursor()
        if self.db_type == 'postgresql':
            cursor.execute('''
                INSERT INTO categories (name, description, color, created_by)
                VALUES (%s, %s, %s, %s) ON CONFLICT (name) DO UPDATE SET
                description = EXCLUDED.description, color = EXCLUDED.color, created_by = EXCLUDED.created_by
            ''', (name, description, color, created_by))
        else:
            cursor.execute('''
                INSERT OR REPLACE INTO categories (name, description, color, created_by)
                VALUES (?, ?, ?, ?)
            ''', (name, description, color, created_by))
        self.commit()
        return cursor.lastrowid

    def get_categories(self):
        """Get all categories"""
        cursor = self.get_cursor()
        cursor.execute('''
            SELECT id, name, description, color, created_by, created_at
            FROM categories
            ORDER BY name
        ''')
        results = cursor.fetchall()
        if self.db_type == 'postgresql':
            return [dict(r) for r in results]
        else:
            return [{
                'id': r[0],
                'name': r[1], 
                'description': r[2], 
                'color': r[3],
                'created_by': r[4], 
                'created_at': r[5]
            } for r in results]

    def get_category_by_name(self, name):
        """Get a category by name"""
        cursor = self.get_cursor()
        placeholder = '%s' if self.db_type == 'postgresql' else '?'
        cursor.execute(f'''
            SELECT id, name, description, color, created_by, created_at
            FROM categories
            WHERE name = {placeholder}
        ''', (name,))
        result = cursor.fetchone()
        if result:
            if self.db_type == 'postgresql':
                return dict(result)
            else:
                return {
                    'id': result[0],
                    'name': result[1], 
                    'description': result[2], 
                    'color': result[3],
                    'created_by': result[4], 
                    'created_at': result[5]
                }
        return None

    def delete_category(self, category_id):
        """Delete a category by ID"""
        cursor = self.get_cursor()
        placeholder = '%s' if self.db_type == 'postgresql' else '?'
        cursor.execute(f'DELETE FROM categories WHERE id = {placeholder}', (category_id,))
        self.commit()
        return cursor.rowcount > 0

    def assign_video_category(self, video_id, category_id, assigned_by):
        """Assign a category to a video"""
        cursor = self.get_cursor()
        if self.db_type == 'postgresql':
            cursor.execute('''
                INSERT INTO video_categories (video_id, category_id, assigned_by)
                VALUES (%s, %s, %s) ON CONFLICT (video_id, category_id) DO UPDATE SET
                assigned_by = EXCLUDED.assigned_by, assigned_at = CURRENT_TIMESTAMP
            ''', (video_id, category_id, assigned_by))
        else:
            cursor.execute('''
                INSERT OR REPLACE INTO video_categories (video_id, category_id, assigned_by)
                VALUES (?, ?, ?)
            ''', (video_id, category_id, assigned_by))
        self.commit()
        return cursor.rowcount > 0

    def get_video_categories(self, video_id):
        """Get all categories for a video"""
        cursor = self.get_cursor()
        placeholder = '%s' if self.db_type == 'postgresql' else '?'
        cursor.execute(f'''
            SELECT c.id, c.name, c.description, c.color, vc.assigned_by, vc.assigned_at
            FROM categories c
            JOIN video_categories vc ON c.id = vc.category_id
            WHERE vc.video_id = {placeholder}
            ORDER BY c.name
        ''', (video_id,))
        results = cursor.fetchall()
        if self.db_type == 'postgresql':
            return [dict(r) for r in results]
        else:
            return [{
                'id': r[0],
                'name': r[1], 
                'description': r[2], 
                'color': r[3],
                'assigned_by': r[4], 
                'assigned_at': r[5]
            } for r in results]

    def get_videos_by_category(self, category_id):
        """Get all videos in a category"""
        cursor = self.get_cursor()
        placeholder = '%s' if self.db_type == 'postgresql' else '?'
        cursor.execute(f'''
            SELECT v.id, v.file_id, v.name, v.description, v.created_at
            FROM videos v
            JOIN video_categories vc ON v.id = vc.video_id
            WHERE vc.category_id = {placeholder}
            ORDER BY v.name
        ''', (category_id,))
        results = cursor.fetchall()
        if self.db_type == 'postgresql':
            return [dict(r) for r in results]
        else:
            return [{
                'id': r[0],
                'file_id': r[1], 
                'name': r[2], 
                'description': r[3],
                'created_at': r[4]
            } for r in results]

    def remove_video_category(self, video_id, category_id):
        """Remove a category from a video"""
        cursor = self.get_cursor()
        if self.db_type == 'postgresql':
            cursor.execute('DELETE FROM video_categories WHERE video_id = %s AND category_id = %s', 
                         (video_id, category_id))
        else:
            cursor.execute('DELETE FROM video_categories WHERE video_id = ? AND category_id = ?', 
                         (video_id, category_id))
        self.commit()
        return cursor.rowcount > 0