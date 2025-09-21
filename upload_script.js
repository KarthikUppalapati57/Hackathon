const admin = require('firebase-admin');
const fs = require('fs');

// IMPORTANT: Download your Firebase service account key
// 1. Go to your Firebase Project Settings -> Service accounts
// 2. Click "Generate new private key"
// 3. Save the downloaded JSON file in your `Career-Advisor_Backend` folder
// 4. RENAME the downloaded file to `serviceAccountKey.json`
const serviceAccount = require('./serviceAccountKey.json');

// Initialize the Firebase Admin SDK
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();
const companies = JSON.parse(fs.readFileSync('companies.json', 'utf8'));

async function uploadCompanies() {
  console.log('Starting company upload...');
  
  const batch = db.batch();

  companies.forEach(company => {
    // Use the companyName as the document ID for easy reference
    const docRef = db.collection('targetCompanies').doc(company.companyName);
    batch.set(docRef, company);
  });

  try {
    await batch.commit();
    console.log(`Successfully uploaded ${companies.length} companies to Firestore!`);
  } catch (error) {
    console.error('Error uploading companies:', error);
  }
}

uploadCompanies();

