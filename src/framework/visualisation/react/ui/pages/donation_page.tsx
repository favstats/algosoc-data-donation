import React, { useMemo, useState } from 'react'
import { Weak } from '../../../../helpers'
import TextBundle from '../../../../text_bundle'
import { Translator } from '../../../../translator'
import { Translatable } from '../../../../types/elements'
import { PropsUIPageDonation } from '../../../../types/pages'
import { 
  isPropsUIPromptConfirm, 
  isPropsUIPromptConsentForm, 
  isPropsUIPromptFileInput, 
  isPropsUIPromptRadioInput, 
  isPropsUIPromptQuestionnaire, 
  isPropsUIPromptConfirmWithLink, 
  PropsUIPromptConfirm, 
  PropsUIPromptConfirmWithLink 
} from '../../../../types/prompts'
import { ReactFactoryContext } from '../../factory'
import { ForwardButton } from '../elements/button'
import { Title1 } from '../elements/text'
import { Confirm } from '../prompts/confirm'
import { ConsentForm } from '../prompts/consent_form'
import { FileInput } from '../prompts/file_input'
import { Questionnaire } from '../prompts/questionnaire'
import { RadioInput } from '../prompts/radio_input'
import { Footer } from './templates/footer'
import { Page } from './templates/page'
import { Progress } from '../elements/progress'

type Props = Weak<PropsUIPageDonation> & ReactFactoryContext



export const DonationPage = (props: Props): JSX.Element => {
  const { title, forwardButton } = prepareCopy(props)
  const { locale, resolve } = props

  // Move useState and toggleTable inside the component
  const [isExpanded, setIsExpanded] = useState(false);
  const toggleTable = () => {
    setIsExpanded(!isExpanded);
  };


  // Generate the dynamic list of data types
  const uniqueTypes = useMemo(() => {
    if (!isPropsUIPromptConsentForm(props.body)) {
      return []
    }

    const tables = props.body.tables || []
    if (tables.length === 0) {
      return []
    }

    const firstTable = tables[0]
    const data = JSON.parse(firstTable.data_frame)
    
    // Extract unique "Type" values and assert them as strings
    const types = Object.values(data.Type || {}) as string[]
    return Array.from(new Set(types))  // Return unique types
  }, [props.body])


  const typeDescriptions: { [key: string]: string } = {
    'Advertentie Info': 'De advertenties waar u interactie mee hebt gehad en uw advertentie voorkeuren.',
    'Advertentie Data': 'De advertenties waar u interactie mee hebt gehad en uw advertentie voorkeuren.',
    'Gevolgde Accounts': 'De accounts die u volgt op deze sociale media.',
    'Hashtags': 'De hashtags die u gebruikt hebt.',
    'Kijkgeschiedenis': "De video's die u bekeken hebt.",
    'Likes': 'De video’s die u hebt geliket.',
    'Reacties': 'Uw reacties op berichten van anderen, inclusief de gebruikersnaam van de persoon die het   bericht heeft gepost.',
    'Shares': 'De video’s die u hebt gedeeld en op welke manier u deze gedeeld hebt.',
    'Zoekopdrachten': 'De zoekopdrachten die u gedaan hebt.',
    'Browsergeschiedenis': 'De websites die u met Google Chrome heeft bezocht op alle apparaten die aan uw   Google-account zijn gekoppeld.',
    'Google News': 'De artikelen die u via Google News hebt bekeken en de meldingen die u van Google News hebt   ontvangen.',
    'Nieuwsbetrokkenheid': 'Nieuwsartikelen die u hebt gelezen op externe websites bijv. de Telegraaf of   Volkskrant.',
    'Video Zoekopdrachten': 'De exacte zoekopdrachten naar video’s die u hebt gedaan via Google, en de video’s   die u hebt bekeken via deze zoekopdrachten.',
    'YouTube Kijkgeschiedenis': 'De video’s die u hebt bekeken op YouTube.',
    'YouTube Reacties': 'Uw exacte reacties op YouTube-video’s van anderen, inclusief de video waarop u   gereageerd heeft.',
    'YouTube Abonnementen': 'De YouTube-kanalen waarop u geabonneerd bent.',
    
    'Gelikete Posts': 'De posts van anderen die u hebt geliket of op gereageerd hebt.',
    'Posts': 'De posts die u hebt geplaatst inclusief de tekst die u gebruikt hebt maar zonder eventuele foto’s   of video’s.',
    'Groepspost': 'De posts die u hebt geplaatst in besloten groepen inclusief de tekst die u gebruikt hebt   maar zonder eventuele foto’s of video’s. Bevat ook de naam van de besloten groep waarin u gereageerd hebt.',
    'Groepsreactie': 'Uw reacties op berichten van anderen in besloten groepen, inclusief de gebruikersnaam   van de persoon die het bericht heeft gepost. Bevat ook de naam van de besloten groep waarin u gereageerd hebt  .',
    'Groepslidmaatschap': 'De besloten groepen waar u lid bent van geworden of hebt verlaten.',
    'Volgsuggesties': 'De accounts die door het platform aan u voorgesteld zijn om te volgen.',
    'Onlangs bezocht': 'De pagina’s, profielen, evenementen of groepen die u onlangs bezocht hebt op Facebook.'  ,
    
    'Events': 'De Facebook-evenementenpagina’s die u bezocht of bekeken hebt.',
    'Subscription Status': 'Uw eventuele abonnementsstatus, zoals het afmelden voor advertenties.',
    
    'Reels': 'De reels die u hebt geplaatst inclusief de tekst die u gebruikt hebt maar zonder eventuele foto’s   of video’s.',
    'Gelikete Stories': 'De accounts waarvan u stories hebt geliket.',
    'Posts die zijn bekeken': 'De posts die u hebt bekeken, inclusief een link naar de post.',
    'Vind ik leuk Reacties': 'Reacties die u hebt geliket, inclusief een link naar de reactie.',
    'Posts gezien': 'De posts die u hebt bekeken, inclusief de gebruikersnaam van de persoon die de post   geplaatst heeft.',
    'Advertenties gezien': 'De advertenties die u hebt bekeken, inclusief de gebruikersnaam van de persoon die   de advertentie geplaatst heeft.',
    
    'Favoriete Videos': 'De video’s die u als favoriet hebt opgeslagen.',
    'Favoriete Hashtags': 'De hashtags die u als favoriet hebt opgeslagen.',
    'Stories': 'De stories die u hebt geplaatst inclusief de tekst die u gebruikt hebt maar zonder eventuele foto’s of video’s.',
    'Aangeklikte Advertenties': 'De advertenties die u hebt aangeklikt.',
    'Google Discover': 'Google Discover inhoud die u hebt aangeklikt.',
    'Advertentie-interactie': 'De advertenties waar u op gereageerd hebt.'
    
    


  }



  const dynamicTable = (
    <div>
      <button
        onClick={toggleTable}
        style={{
          cursor: 'pointer',
          marginBottom: '1em',
          display: 'flex',
          alignItems: 'center',
          border: 'none',
          background: 'none',
          padding: 0,
          fontSize: '1em',
          color: '#007BFF'
        }}
      >
        {isExpanded ? 'Verberg gegevensdetails' : 'Klik hier voor meer informatie over de gegevens die u zou doneren'}
        <span
          style={{
            display: 'inline-block',
            marginLeft: '0.5em',
            transition: 'transform 0.3s',
            transform: isExpanded ? 'rotate(90deg)' : 'rotate(0deg)'
          }}
        >
          ▶
        </span>
      </button>
  
      {isExpanded && (
        <>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr>
                <th style={{ textAlign: 'left', padding: '0.5em', borderBottom: '1px solid #ddd' }}>Type</th>
                <th style={{ textAlign: 'left', padding: '0.5em', borderBottom: '1px solid #ddd' }}>Beschrijving</th>
              </tr>
            </thead>
            <tbody>
              {uniqueTypes.length > 0 ? (
                uniqueTypes.sort().map((type: string, index: number) => (
                  <tr key={index}>
                    <td style={{ padding: '0.5em', borderBottom: '1px solid #ddd' }}>
                      <strong><em>{type}</em></strong>
                    </td>
                    <td style={{ padding: '0.5em', borderBottom: '1px solid #ddd' }}>
                      {typeDescriptions[type] ? typeDescriptions[type] : '(Geen beschrijving beschikbaar)'}
                    </td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={2} style={{ padding: '0.5em', textAlign: 'center' }}>
                    Geen relevante gegevenstypen beschikbaar om weer te geven.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </>
      )}
    <br /> {/* Add a line break here */}
    </div>
  );





  function renderBody (props: Props): JSX.Element {
    const context = { locale: locale, resolve: props.resolve }
    const body = props.body
  
    if (isPropsUIPromptFileInput(body)) {
      return <FileInput {...body} {...context} />
    }
    if (isPropsUIPromptConfirm(body) || isPropsUIPromptConfirmWithLink(body)) {
      return <Confirm {...body} {...context} />
    }
    if (isPropsUIPromptConsentForm(body)) {
      return <ConsentForm {...body} locale={locale} resolve={resolve} dynamicList={dynamicTable} />
    }
    if (isPropsUIPromptRadioInput(body)) {
      return <RadioInput {...body} {...context} />
    }
    if (isPropsUIPromptQuestionnaire(body)) {
      return <Questionnaire {...body} {...context} />
    }
    throw new TypeError('Unknown body type')
  }

  function handleSkip (): void {
    resolve?.({ __type__: 'PayloadFalse', value: false })
  }

  function renderFooter (props: Props): JSX.Element | undefined {
    if (props.footer != null) {
      return <Footer
        middle={<Progress percentage={props.footer?.progressPercentage ?? 0} />}
        right={
          <div className="flex flex-row">
            <div className="flex-grow" />
            <ForwardButton label={forwardButton} onClick={handleSkip} />
          </div>
        }
      />
    }
    return undefined
  }

  const body = (
    <>
      <Title1 text={title} />
      {renderBody(props)}
    </>
  )

  return <Page body={body} footer={renderFooter(props)} />
}

interface Copy {
  title: string
  forwardButton: string
}

function prepareCopy ({ header: { title }, locale }: Props): Copy {
  return {
    title: Translator.translate(title, locale),
    forwardButton: Translator.translate(forwardButtonLabel(), locale)
  }
}

const forwardButtonLabel = (): Translatable => {
  return new TextBundle().add('en', 'Skip').add('nl', 'Overslaan')
}
