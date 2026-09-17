const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const multer = require('multer');
const { createClient } = require('@libsql/client');

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] },
    transports: ['websocket', 'polling']
});

const db = createClient({ url: "file:chat.db" });

async function initDB() {
    await db.execute(`CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT,
        text TEXT,
        filePath TEXT,
        fileName TEXT,
        replyToUser TEXT,
        replyToText TEXT,
        created_at INTEGER
    )`);
    console.log('Database SQLite siap.');
}
initDB().catch(console.error);

// Setup multer - simpan file ke public/uploads/
const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        cb(null, path.join(__dirname, 'public/uploads'));
    },
    filename: (req, file, cb) => {
        const unique = Date.now() + '-' + Math.round(Math.random() * 1e6);
        const ext = path.extname(file.originalname);
        cb(null, unique + ext);
    }
});
const upload = multer({
    storage,
    limits: { fileSize: 100 * 1024 * 1024 } // 100MB
});

app.use(express.static(path.join(__dirname, 'public')));

// Endpoint upload file
app.post('/upload', upload.single('file'), (req, res) => {
    if (!req.file) return res.status(400).json({ error: 'Tidak ada file' });
    res.json({ filePath: '/uploads/' + req.file.filename });
});

const activeUsers = {};

io.on('connection', async (socket) => {
    socket.on('check username', (username, callback) => {
        if (activeUsers[username]) {
            callback({ success: false, message: "Username sudah dipakai!" });
        } else {
            callback({ success: true });
        }
    });

    socket.on('join chat', (username) => {
        activeUsers[username] = socket.id;
        io.emit('update user list', Object.keys(activeUsers));
    });

    try {
        const result = await db.execute("SELECT id, username, text, filePath, fileName, replyToUser, replyToText, created_at FROM messages ORDER BY id ASC");
        socket.emit('chat history', result.rows);
    } catch (err) { console.error(err.message); }

    socket.on('chat message', async (data) => {
        const { username, text, filePath, fileName, replyToUser, replyToText } = data;
        const nowMs = Date.now();
        try {
            const res = await db.execute({
                sql: "INSERT INTO messages (username, text, filePath, fileName, replyToUser, replyToText, created_at) VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING id",
                args: [username, text, filePath || '', fileName || '', replyToUser || '', replyToText || '', nowMs]
            });

            const insertedId = res.rows[0].id || res.rows[0][0];
            const msgObj = {
                id: insertedId,
                username, text,
                filePath: filePath || '',
                fileName: fileName || '',
                replyToUser: replyToUser || '',
                replyToText: replyToText || '',
                created_at: nowMs
            };
            io.emit('chat message', msgObj);
        } catch (err) { console.error(err.message); }
    });

    socket.on('edit message', async (data) => {
        const { id, newText, username } = data;
        try {
            await db.execute({
                sql: "UPDATE messages SET text = ? WHERE id = ? AND username = ?",
                args: [newText, id, username]
            });
            io.emit('message edited', { id, newText });
        } catch (err) { console.error(err.message); }
    });

    socket.on('delete message', async (data) => {
        const { id, username } = data;
        try {
            const check = await db.execute({
                sql: "SELECT created_at FROM messages WHERE id = ? AND username = ?",
                args: [id, username]
            });

            if (check.rows.length > 0) {
                const createdAt = check.rows[0].created_at || check.rows[0][0];
                const nowMs = Date.now();
                if ((nowMs - createdAt) <= 60000) {
                    await db.execute({ sql: "DELETE FROM messages WHERE id = ?", args: [id] });
                    io.emit('message deleted', { id });
                } else {
                    socket.emit('delete error', 'Gagal! Pesan sudah lewat dari 1 menit.');
                }
            }
        } catch (err) { console.error(err.message); }
    });

    socket.on('kick user', (targetUsername) => {
        const targetSocketId = activeUsers[targetUsername];
        if (targetSocketId) {
            io.to(targetSocketId).emit('you are kicked');
            delete activeUsers[targetUsername];
            io.emit('update user list', Object.keys(activeUsers));
        }
    });

    socket.on('disconnect', () => {
        for (const username in activeUsers) {
            if (activeUsers[username] === socket.id) {
                delete activeUsers[username];
                break;
            }
        }
        io.emit('update user list', Object.keys(activeUsers));
    });
});

const PORT = process.env.PORT || 80;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`Server jalan di http://localhost:${PORT}`);
});
