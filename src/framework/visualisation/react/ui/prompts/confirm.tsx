// confirm.tsx

import { Weak } from '../../../../helpers'
import { ReactFactoryContext } from '../../factory'
import { PropsUIPromptConfirm, PropsUIPromptConfirmWithLink } from '../../../../types/prompts'
import { Translator } from '../../../../translator'
import { BodyLarge } from '../elements/text'
import { PrimaryButton } from '../elements/button'
import { Translatable } from '../../../../types/elements'  // Add this import

type Props = (
  | (Weak<PropsUIPromptConfirm> & ReactFactoryContext)
  | (Weak<PropsUIPromptConfirmWithLink> & ReactFactoryContext)
)

export const Confirm = (props: Props): JSX.Element => {
  const { resolve, locale } = props
  const { text, ok, cancel } = prepareCopy(props)

  const linkProps = 'link_text' in props && 'link_url' in props
    ? { link_text: props.link_text, link_url: props.link_url }
    : undefined

  const optionalText = 'optional_text' in props ? props.optional_text : undefined

  function handleOk (): void {
    resolve?.({ __type__: 'PayloadTrue', value: true })
  }

  function handleCancel (): void {
    resolve?.({ __type__: 'PayloadFalse', value: false })
  }

  return (
    <>
      <BodyLarge text={text} margin='mb-4' />
      {linkProps && (
        <div className="text-center mb-4">
          <a 
            href={linkProps.link_url} 
            target="_blank" 
            rel="noopener noreferrer" 
            className="text-primary hover:underline text-3xl"
          >
            {Translator.translate(linkProps.link_text, locale)}
          </a>
        </div>
      )}
      {optionalText && (
        <p className="mb-4 text-sm">
          {Translator.translate(optionalText, locale)}
        </p>
      )}
      <div className='flex flex-row gap-4'>
        {ok !== '' && <PrimaryButton label={ok} onClick={handleOk} color='text-white bg-primary' />}
        {cancel !== '' && <PrimaryButton label={cancel} onClick={handleCancel} color='text-grey1 bg-tertiary' />}
      </div>
    </>
  )
}

interface Copy {
  text: string
  link_text?: string
  link_url?: string
  ok: string
  cancel: string
}

function prepareCopy (props: Props): Copy {
  const { text, ok, cancel, locale } = props
  return {
    text: Translator.translate(text, locale),
    ok: Translator.translate(ok, locale),
    cancel: Translator.translate(cancel, locale)
  }
}

