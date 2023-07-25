package com.astorage.utils.clinvar;

import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.*;

import java.util.ArrayList;

public class ClinVarXMLParser extends DefaultHandler {
	Submitter lastSubmitter;
	Significance lastSignificance;
	ArrayList<Submitter> submitters = new ArrayList<>();
	ArrayList<Significance> significances = new ArrayList<>();
	boolean isReferenceBlock = false;
	boolean isClinicalSignificanceDescriptionBlock = false;
	public void startElement(String uri, String localName, String qName, Attributes attributes) {
		switch (qName) {
			case "ClinVarSet":
				this.lastSubmitter = new Submitter();
				this.lastSignificance = new Significance();
			case "ReferenceClinVarAssertion":
				this.isReferenceBlock = true;
				break;
			case "ClinVarAssertion":
				this.isReferenceBlock = false;
				break;
			case "ClinVarSubmissionID":
				this.lastSubmitter.setSubmitterName(attributes.getValue("submitter"));
				break;
			case "ClinVarAccession":
				if (this.isReferenceBlock) {
					this.lastSignificance.setRCVAccession(attributes.getValue("Acc"));
				} else {
					this.lastSignificance.setSubmitterId(attributes.getValue("OrgID"));
					this.lastSubmitter.setSubmitterId(attributes.getValue("OrgID"));
				}
				break;
			case "Description":
				if (!this.isReferenceBlock) {
					this.isClinicalSignificanceDescriptionBlock = true;
				}
		}
	}

	public void endElement(String uri, String localName, String qName) throws SAXException {
		if (qName.equals("ClinVarSet")) {
			this.significances.add(this.lastSignificance);
			this.submitters.add(this.lastSubmitter);
		}
	}

	public void characters(char[] ch, int start, int length) throws SAXException {
		if (this.isClinicalSignificanceDescriptionBlock) {
			this.lastSignificance.setClinicalSignificance(new String(ch, start, length));
		}
	}

	public ArrayList<Submitter> getSubmitters() {
		return this.submitters;
	}

	public ArrayList<Significance> getSignificances() {
		return this.significances;
	}
}
