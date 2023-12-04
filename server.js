const express = require('express');
const { Client } = require('cassandra-driver');
const cors = require('cors');
const multer = require('multer');
const bodyParser = require('body-parser');

const app = express();
const port = 3000;

app.use(cors());
// app.use(express.json());
app.use(bodyParser.json({limit: '1000mb'}));

const storage = multer.memoryStorage();
const upload = multer({ storage: storage, limits: { fileSize: 100000000000 } });

const client = new Client({
    cloud: { secureConnectBundle: './extras/secure-connect-contemporaryproject.zip' },
    credentials: {
      username: 'pXFMlrqHCMYjZtHHGWWSmMsi',
      password: 'F8UOtYelFa1AsBWo,9O7ia_2M4e7CBb3,RNM9tYshO+zEGh-Ykoz4or5xA27Q_73Ctkxb.76TFNkYRv18wo_D_EWF2o0ZS2EqmZHo,.59YBZkHgTlmEzc,N1PSTH3r+y'
    },
    keyspace: 'tabular'
});

async function connectToCassandra() {
    try {
      await client.connect();
      console.log('Connected to Cassandra');
    } catch (error) {
      console.error('Error connecting to Cassandra:', error);
    }
  }
  
  connectToCassandra();

  app.get('/api/get-student', async (req, res) => {
    try {
      const result = await client.execute('SELECT * FROM student_info');
      console.log(result.rows);
      const data = res.json(result.rows);

      console.log(data);

    } catch (error) {
      console.error('Error executing Cassandra query:', error);
      res.status(500).json({ error: 'Internal Server Error' });
    }
  });

  app.post('/api/insert-student', upload.single('image'), async(req, res) => {
    try {
        const { studid, stud_fname, stud_lname, stud_password, stud_username } = req.body;
        const imageBuffer = req.file.buffer;

        const decodeImage = Buffer.from(imageBuffer, 'base64');

        const query = 'INSERT INTO student_info(studid, stud_fname, stud_lname, stud_password, stud_username, image) VALUES (?, ?, ?, ?, ?, ?)';
        await client.execute(query, [studid, stud_fname, stud_lname, stud_password, stud_username,  imageBuffer]);

        res.status(201).json({ message: 'Data entry successful'});

    }catch(error) {
        console.error(error);
    }
  });


  // Start the server
  app.listen(port, () => {
    console.log(`Server listening at http://localhost:${port}`);
  });