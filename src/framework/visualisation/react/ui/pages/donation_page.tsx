import React, { useMemo } from 'react'
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
    'Advertentie Info': '(this is data about your advertisements)',
    'Gevolgde Accounts': '(these are the accounts you follow)',
    'Hashtags': '(this is the hashtags you have used)',
    'Kijkgeschiedenis': '(this is your video watch history)',
    'Likes': '(these are your liked content)',
    'Reacties': '(these are your comments)',
    'Shares': '(this represents shared content)',
    'Zoekopdrachten': '(this is your search history)',
    'Browsergeschiedenis': '(this is your browser history from Chrome)',
    'Google News': '(this is data about your activity on Google News)',
    'Nieuwsbetrokkenheid': '(this represents your engagement with news articles)',
    'Video Zoekopdrachten': '(this is your video search history)',
    'YouTube Kijkgeschiedenis': '(this is your YouTube watch history)',
    'YouTube Reacties': '(these are the comments you made on YouTube videos)',
    'YouTube Abonnementen': '(this represents your YouTube subscriptions)',
    
    'Gelikete Posts': '(this is data about posts you liked or reacted to)',
    'Posts': '(this is data about posts you made)',
    'Groepspost': '(this is data about your posts in groups)',
    'Groepsreactie': '(this is data about your comments in groups)',
    'Groepslidmaatschap': '(this is data about the groups you have joined)',
    'Volgsuggesties': '(this is data about accounts suggested to you to follow)',
    'Onlangs bezocht': '(this is data about pages, profiles, events, or groups you recently visited)',
    'AdPreference': '(this is data about your preferences regarding advertisements)',
    'Info Used to Target You': '(this is data used by advertisers to target you)',
    'Events': '(this is data about events you interacted with)',
    'Subscription Status': '(this is data about your subscription status, such as for opting out of ads)',
    // Instagram-Specific Types
    'Reels': '(this is data about Reels you posted or watched)',
    'Gelikete Stories': '(this is data about stories you liked)',
    'Posts die zijn bekeken': '(this is data about posts you have seen)',


  }

  // Create the dynamic list for the consent form with bullet points and adjust spacing
  const dynamicList = uniqueTypes.length > 0 ? (
    <ul style={{ marginBottom: '1.5em', paddingLeft: '1.2em' }}>
      {uniqueTypes.map((type: string, index: number) => (
        <li key={index} style={{ listStyleType: 'disc', marginBottom: '0.2em' }}>
          {type} {typeDescriptions[type] || '(no description available)'}
        </li>
      ))}
    </ul>
  ) : <p>Geen relevante gegevenstypen beschikbaar om weer te geven.</p>


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
      return <ConsentForm {...body} locale={locale} resolve={resolve} dynamicList={dynamicList} />
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
